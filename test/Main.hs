-- | Runs real hasql sessions through a traced adapter against a live Postgres
-- and inspects the spans that come out. The connection is configured by the
-- usual libpq environment (@PGHOST@, @PGPORT@, @PGUSER@, @PGPASSWORD@,
-- @PGDATABASE@); without @PGHOST@ the suite is skipped.
module Main (main) where

import Control.Exception (bracket)
import Control.Monad (forM_, void)
import Data.Foldable (for_)
import Data.IORef (IORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List (sortOn)
import Data.Text (Text)
import Data.Text qualified as T
import Hasql.Connection qualified as Connection
import Hasql.Connection.Settings qualified as Settings
import Hasql.Decoders qualified as Decoders
import Hasql.Encoders qualified as Encoders
import Hasql.OpenTelemetry (Config (..), defaultConfig, tracedWith)
import Hasql.Session qualified as Session
import Hasql.Statement qualified as Statement
import OpenTelemetry.Attributes (Attribute (..), PrimitiveAttribute (..), lookupAttribute)
import OpenTelemetry.Context qualified as Context
import OpenTelemetry.Context.ThreadLocal qualified as ThreadLocal
import OpenTelemetry.Exporter.InMemory.Span (inMemoryListExporter)
import OpenTelemetry.Trace.Core qualified as Otel
import Pqi qualified
import Pqi.Ffi qualified
import Pqi.Native qualified
import System.Environment (lookupEnv)
import Test.Hspec


main :: IO ()
main =
    lookupEnv "PGHOST" >>= \case
        Nothing -> putStrLn "PGHOST is not set, skipping the integration tests"
        Just _ -> do
            (processor, exported) <- inMemoryListExporter
            provider <- Otel.createTracerProvider [processor] Otel.emptyTracerProviderOptions
            Otel.setGlobalTracerProvider provider
            hspec do
                for_ [Pqi.Ffi.adapter, Pqi.Native.adapter] \adapter ->
                    describe (T.unpack (Pqi.name adapter)) (spec adapter exported)


-- | What a test needs to know about an exported span.
data Exported = Exported
    { name :: Text
    , attribute :: Text -> Maybe Attribute
    , status :: Otel.SpanStatus
    , start :: Otel.Timestamp
    }


spec :: Pqi.Adapter -> IORef [Otel.ImmutableSpan] -> Spec
spec adapter exported = do
    it "reports nothing outside a parent span" do
        spans <- capture exported do
            withConnection defaultConfig adapter \c -> run c (Session.statement 1 addOne)
        map name spans `shouldBe` []

    it "reports one span per command, with the statement on each" do
        spans <- capture exported do
            withConnection defaultConfig adapter \c -> underParent do
                forM_ [1, 2, 3] \n -> run c (Session.statement n addOne)
        -- The first execution prepares the statement; the rest reuse it.
        map name spans `shouldBe` ["PREPARE SELECT", "SELECT", "SELECT", "SELECT"]
        for_ spans \s -> do
            text (attribute s "db.query.text") `shouldBe` Just "select $1 :: int8 + 1"
            text (attribute s "db.system.name") `shouldBe` Just "postgresql"
            attribute s "db.postgresql.results_unread" `shouldBe` Nothing
            status s `shouldBe` Otel.Unset
        int (attribute (spans !! 1) "db.response.returned_rows") `shouldBe` Just 1

    it "records the SQLSTATE of a failure and none of its message" do
        spans <- capture exported do
            withConnection defaultConfig adapter \c -> underParent do
                void (Connection.use c (Session.statement "not a uuid" castToUuid))
                run c (Session.statement 41 addOne)
        let failed = filter (\s -> status s /= Otel.Unset) spans
        map (text . (`attribute` "db.response.status_code")) failed `shouldBe` [Just "22P02"]
        map status failed `shouldBe` [Otel.Error "postgres reported an error, SQLSTATE 22P02"]
        -- The value that failed the cast must not appear anywhere.
        for_ spans \s -> for_ ["db.query.text", "db.response.status_code", "error.type"] \key ->
            fmap (T.isInfixOf "not a uuid") (text (attribute s key)) `shouldNotBe` Just True
        -- And the queue is still aligned for the statement after the failure.
        map name (filter (\s -> text (attribute s "db.query.text") == Just "select $1 :: int8 + 1") spans)
            `shouldSatisfy` (\names -> "SELECT" `elem` names)
        for_ spans \s -> attribute s "db.postgresql.results_unread" `shouldBe` Nothing

    it "shapes spans for Datadog when asked" do
        spans <- capture exported do
            withConnection defaultConfig{datadogCompat = True} adapter \c -> underParent do
                run c (Session.statement 1 addOne)
        map name spans `shouldBe` ["postgresql.prepare", "postgresql.query"]
        for_ spans \s -> do
            text (attribute s "span.type") `shouldBe` Just "sql"
            text (attribute s "resource.name") `shouldBe` Just "select $1 :: int8 + 1"
            text (attribute s "db.statement") `shouldBe` Just "select $1 :: int8 + 1"

    it "flattens and cuts long statements" do
        spans <- capture exported do
            withConnection defaultConfig{maxStatementLength = 10} adapter \c -> underParent do
                run c (Session.script "select\n    1")
        map (text . (`attribute` "db.query.text")) spans `shouldBe` [Just "select 1"]
        spans' <- capture exported do
            withConnection defaultConfig{maxStatementLength = 6} adapter \c -> underParent do
                run c (Session.script "select\n    1")
        map (text . (`attribute` "db.query.text")) spans' `shouldBe` [Just "select…"]


addOne :: Statement.Statement Int64 Int64
addOne =
    Statement.preparable
        "select $1 :: int8 + 1"
        (Encoders.param (Encoders.nonNullable Encoders.int8))
        (Decoders.singleRow (Decoders.column (Decoders.nonNullable Decoders.int8)))


castToUuid :: Statement.Statement Text ()
castToUuid =
    Statement.preparable
        "select $1 :: text :: uuid"
        (Encoders.param (Encoders.nonNullable Encoders.text))
        Decoders.noResult


withConnection :: Config -> Pqi.Adapter -> (Connection.Connection -> IO a) -> IO a
withConnection config adapter =
    bracket
        do
            settings <- settingsFromEnv
            Connection.acquire (tracedWith config adapter) settings >>= either (fail . show) pure
        Connection.release


-- | The libpq environment, spelled out because not every adapter reads it.
settingsFromEnv :: IO Settings.Settings
settingsFromEnv = do
    host <- env "PGHOST" "localhost"
    port <- env "PGPORT" "5432"
    user <- env "PGUSER" "postgres"
    password <- env "PGPASSWORD" ""
    database <- env "PGDATABASE" "postgres"
    pure $
        mconcat
            [ Settings.hostAndPort host (read (T.unpack port))
            , Settings.user user
            , Settings.password password
            , Settings.dbname database
            ]
  where
    env key fallback = maybe fallback T.pack <$> lookupEnv key


run :: Connection.Connection -> Session.Session a -> IO ()
run c session = Connection.use c session >>= either (fail . show) (const (pure ()))


-- | Run under a fresh root span, as an application's request handler would.
underParent :: IO a -> IO a
underParent act = do
    provider <- Otel.getGlobalTracerProvider
    let tracer = Otel.makeTracer provider "test" Otel.tracerOptions
    bracket
        (Otel.createSpan tracer Context.empty "parent" Otel.defaultSpanArguments)
        (\parent -> Otel.endSpan parent Nothing >> ThreadLocal.adjustContext (const Context.empty))
        \parent -> do
            ThreadLocal.adjustContext (Context.insertSpan parent)
            act


-- | The database spans an action reported, in the order they started.
capture :: IORef [Otel.ImmutableSpan] -> IO () -> IO [Exported]
capture exported act = do
    writeIORef exported []
    act
    spans <- readIORef exported
    described <- traverse describeSpan spans
    pure . sortOn start $ filter (\s -> name s /= "parent") described
  where
    describeSpan s = do
        hot <- readIORef (Otel.spanHot s)
        pure
            Exported
                { name = Otel.hotName hot
                , attribute = lookupAttribute (Otel.hotAttributes hot)
                , status = Otel.hotStatus hot
                , start = Otel.spanStart s
                }


text :: Maybe Attribute -> Maybe Text
text = \case
    Just (AttributeValue (TextAttribute t)) -> Just t
    _ -> Nothing


int :: Maybe Attribute -> Maybe Int64
int = \case
    Just (AttributeValue (IntAttribute i)) -> Just i
    _ -> Nothing
