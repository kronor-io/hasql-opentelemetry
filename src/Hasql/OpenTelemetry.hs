-- | OpenTelemetry tracing for hasql, by decorating its pqi adapter.
--
-- 'Pqi.Adapter' and 'Pqi.Connection' are records of @IO@ closures rather than
-- opaque handles, so an adapter can be wrapped: 'traced' takes whichever
-- backend you picked (@pqi-ffi@ or @pqi-native@) and returns one whose
-- connections open a client span per command. Nothing above changes — hasql
-- keeps calling the same fields — and the span carries what an application
-- level span cannot: the SQL that actually went to Postgres.
--
-- > import Hasql.Connection qualified as Connection
-- > import Hasql.OpenTelemetry qualified as HasqlOtel
-- > import Pqi.Ffi qualified
-- >
-- > Connection.acquire (HasqlOtel.traced Pqi.Ffi.adapter) settings
--
-- The same adapter goes to @hasql-pool@. Spans are reported to the global
-- tracer provider, and by default only under a span that is already active on
-- the calling thread (see 'requireParentSpan').
--
-- == Where the SQL comes from
--
-- hasql 2 sends a statement twice over: once as @sendPrepare name sql@ and
-- from then on as @sendQueryPrepared name params@, which never names the SQL
-- again. The prepare is therefore the only place a connection ever sees the
-- text, and the attributes derived from it are cached per statement name for
-- the life of the connection.
--
-- == How a span is timed
--
-- Every hasql roundtrip is pipelined: @enterPipelineMode@, the sends,
-- @pipelineSync@, then a @getResult@ loop. A send therefore returns long
-- before the server has answered, so timing the send alone would measure
-- nothing. Instead a span starts at the send and is queued; @getResult@
-- returning 'Nothing' — libpq's end-of-this-command marker, in pipeline mode
-- and out of it — ends the one at the head of the queue. Commands are
-- answered in the order they were sent, so the queue stays aligned as long as
-- every result-producing send is wrapped, which is why all of them are, not
-- just the ones hasql happens to call today.
--
-- Interrupted sessions break that pairing: hasql drains and cancels, and a
-- drain reads one 'Nothing' for a pipeline that may have had several commands
-- in it. Leaving pipeline mode therefore closes whatever is still queued,
-- marked @db.postgresql.results_unread@, which bounds a leak at one roundtrip.
--
-- == What is never recorded
--
-- Parameter values, and no text at all from a server error report. hasql
-- passes parameters out of band of the SQL, and they routinely hold personal
-- data and secrets; only the statement text, which is constant for a
-- statement written in the source, reaches a span. A failure records the
-- SQLSTATE and stops there, because every human-readable field of the report
-- can carry the offending value — not just the detail. Postgres puts it in
-- the primary message whenever a cast fails (@invalid input syntax for type
-- uuid: \"...\"@), and a @raise exception \'... %\'@ in PL\/pgSQL
-- interpolates whatever it is given into the same field.
--
-- Statement text built at runtime with literals spliced into it is recorded
-- as sent. Keep such values in parameters.
module Hasql.OpenTelemetry (
    traced,
    tracedWith,
    Config (..),
    defaultConfig,
) where

import Control.Exception (bracket, mask, onException)
import Control.Monad (when, (>=>))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Data.Char qualified as Char
import Data.Foldable (for_, traverse_)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (catMaybes, fromMaybe, isNothing)
import Data.Sequence (Seq)
import Data.Sequence qualified as Seq
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8Lenient)
import OpenTelemetry.Attributes (Attribute, ToAttribute (toAttribute))
import OpenTelemetry.Context qualified as Context
import OpenTelemetry.Context.ThreadLocal qualified as ThreadLocal
import OpenTelemetry.Trace.Core (Span, Tracer)
import OpenTelemetry.Trace.Core qualified as Otel
import Pqi qualified
import Text.Read (readMaybe)


-- | How spans are reported. Start from 'defaultConfig' and override fields.
data Config = Config
    { tracerName :: Text
    -- ^ The instrumentation scope spans are reported under.
    , maxStatementLength :: Int
    -- ^ Longer statements are cut to this many characters. The text is sent
    -- on every execution of a statement, so this is what a span costs in
    -- ingestion.
    , requireParentSpan :: Bool
    -- ^ Only trace commands issued under a span that is already active on the
    -- calling thread. Keeps the volume proportional to traced work: pool
    -- warm-up, @LISTEN@ connections and health-check pings would otherwise
    -- each start a trace of their own.
    , datadogCompat :: Bool
    -- ^ Shape spans the way Datadog APM renders database calls: span names
    -- @postgresql.query@ \/ @postgresql.prepare@ \/ @postgresql.describe@,
    -- @span.type=sql@, and the statement also as @resource.name@ and
    -- @db.statement@. Datadog reads @span.type@ off the span before deriving
    -- one, and @sql@ is what makes it show the statement as a query; it
    -- derives a resource from @db.statement@ only after @resource.name@ has
    -- failed to supply one, so both are set.
    }


-- | Semantic-convention span names and attributes, spans only under an
-- existing parent, statements cut at 1024 characters.
defaultConfig :: Config
defaultConfig =
    Config
        { tracerName = "hasql-opentelemetry"
        , maxStatementLength = 1024
        , requireParentSpan = True
        , datadogCompat = False
        }


-- | Decorate an adapter so that every connection it opens reports its
-- commands to OpenTelemetry, with 'defaultConfig'.
traced :: Pqi.Adapter -> Pqi.Adapter
traced = tracedWith defaultConfig


-- | 'traced' with an explicit t'Config'.
tracedWith :: Config -> Pqi.Adapter -> Pqi.Adapter
tracedWith config adapter =
    adapter
        { Pqi.connectdb = Pqi.connectdb adapter >=> instrument config
        , Pqi.connectStart = Pqi.connectStart adapter >=> instrument config
        }


-- | Per-connection tracing state. One of these is closed over by the wrapped
-- closures of a single 'Pqi.Connection'.
data ConnectionTracer = ConnectionTracer
    { config :: Config
    , tracer :: Tracer
    , connection :: Pqi.Connection
    -- ^ The undecorated connection, which is what every wrapper calls through
    -- to.
    , connectionAttributes :: IORef (Maybe [(Text, Attribute)])
    -- ^ Read from the connection on first use. Deferred rather than read at
    -- connect time because @connectStart@ returns before the connection
    -- exists.
    , statements :: IORef (HashMap ByteString Statement)
    -- ^ Each prepared statement, by the name it was prepared under.
    -- Deliberately unbounded: hasql's own statement cache is per-connection
    -- and unbounded too and goes on executing under the names it holds, so
    -- evicting here would silently strip the SQL off statements that are
    -- still running. Both are bounded by the number of distinct statements in
    -- the program and the connection's lifetime, and 'forget' clears this one
    -- where hasql resets its own.
    , pending :: IORef (Seq (Maybe Span))
    -- ^ Commands sent whose results have not been read yet, oldest first.
    -- 'Nothing' for a command that is not being traced, which still has to
    -- take its turn or the queue stops matching the results.
    }


-- | What the span of a command learns from its SQL.
data Statement = Statement
    { operation :: Maybe Text
    , attributes :: [(Text, Attribute)]
    }


data Command = Query | Prepare | Describe


-- | How many unanswered commands to keep spans for before dropping the
-- oldest. A hasql pipeline holds three; anything beyond that means the queue
-- has lost its place and the span would never be ended otherwise.
maxPendingCommands :: Int
maxPendingCommands = 64


instrument :: Config -> Pqi.Connection -> IO Pqi.Connection
instrument cfg pq
    | Pqi.isNullConnection pq = pure pq
    | otherwise = do
        provider <- Otel.getGlobalTracerProvider
        attributeCache <- newIORef Nothing
        statementCache <- newIORef mempty
        pendingCommands <- newIORef mempty
        let ct =
                ConnectionTracer
                    { config = cfg
                    , tracer = Otel.makeTracer provider (Otel.instrumentationLibrary (tracerName cfg) "") Otel.tracerOptions
                    , connection = pq
                    , connectionAttributes = attributeCache
                    , statements = statementCache
                    , pending = pendingCommands
                    }
            ofSql = pure . parseStatement cfg
            ofName = preparedStatement ct
        pure
            pq
                { Pqi.exec = \sql ->
                    tracedSync ct Query (ofSql sql) (Pqi.exec pq sql)
                , Pqi.execParams = \sql params resultFormat ->
                    tracedSync ct Query (ofSql sql) (Pqi.execParams pq sql params resultFormat)
                , Pqi.prepare = \name sql oids -> do
                    rememberStatement ct name sql
                    tracedSync ct Prepare (ofSql sql) (Pqi.prepare pq name sql oids)
                , Pqi.execPrepared = \name params resultFormat ->
                    tracedSync ct Query (ofName name) (Pqi.execPrepared pq name params resultFormat)
                , Pqi.describePrepared = \name ->
                    tracedSync ct Describe (ofName name) (Pqi.describePrepared pq name)
                , Pqi.describePortal = \name ->
                    tracedSync ct Describe (ofName name) (Pqi.describePortal pq name)
                , Pqi.sendQuery = \sql ->
                    tracedSend ct Query (ofSql sql) (Pqi.sendQuery pq sql)
                , Pqi.sendQueryParams = \sql params resultFormat ->
                    tracedSend ct Query (ofSql sql) (Pqi.sendQueryParams pq sql params resultFormat)
                , Pqi.sendPrepare = \name sql oids -> do
                    rememberStatement ct name sql
                    tracedSend ct Prepare (ofSql sql) (Pqi.sendPrepare pq name sql oids)
                , Pqi.sendQueryPrepared = \name params resultFormat ->
                    tracedSend ct Query (ofName name) (Pqi.sendQueryPrepared pq name params resultFormat)
                , Pqi.sendDescribePrepared = \name ->
                    tracedSend ct Describe (ofName name) (Pqi.sendDescribePrepared pq name)
                , Pqi.sendDescribePortal = \name ->
                    tracedSend ct Describe (ofName name) (Pqi.sendDescribePortal pq name)
                , -- The sync produces a result of its own, so it takes a turn in the
                  -- queue. It is never worth a span: it measures the pipeline, which
                  -- the commands in it already do.
                  Pqi.pipelineSync = do
                    sent <- Pqi.pipelineSync pq
                    when sent (enqueue ct Nothing)
                    pure sent
                , -- Masked because the read advances the connection's protocol state:
                  -- an exception landing between it and the bookkeeping would leave
                  -- the queue one command behind, and every later result would be
                  -- attributed to the wrong statement.
                  Pqi.getResult = mask \restore -> do
                    result <- restore (Pqi.getResult pq)
                    maybe (commandAnswered ct) (resultReceived ct) result
                    pure result
                , Pqi.exitPipelineMode = do
                    left <- Pqi.exitPipelineMode pq
                    when left (abandonPending ct)
                    pure left
                , Pqi.reset = do
                    Pqi.reset pq
                    forget ct
                , Pqi.resetStart = do
                    started <- Pqi.resetStart pq
                    when started (forget ct)
                    pure started
                , Pqi.finish = do
                    abandonPending ct
                    Pqi.finish pq
                }


-- * Spans


-- | The span name for a command. Without 'datadogCompat' it follows the
-- database semantic conventions: the operation where one is known, the
-- database system otherwise.
spanName :: Config -> Command -> Statement -> Text
spanName cfg command statement
    | datadogCompat cfg = case command of
        Query -> "postgresql.query"
        Prepare -> "postgresql.prepare"
        Describe -> "postgresql.describe"
    | otherwise = case command of
        Query -> fromMaybe "postgresql" (operation statement)
        Prepare -> maybe "PREPARE" ("PREPARE " <>) (operation statement)
        Describe -> "DESCRIBE"


-- | Start a span for a command, unless this connection is not being traced
-- right now. The span is deliberately left out of the thread-local context:
-- it is a leaf, and several of them can be open at once on one thread.
startSpan :: ConnectionTracer -> Command -> IO Statement -> IO (Maybe Span)
startSpan ct command mkStatement = do
    context <- ThreadLocal.getContext
    if requireParentSpan (config ct) && isNothing (Context.lookupSpan context)
        then pure Nothing
        else do
            shared <- sharedAttributes ct
            statement <- mkStatement
            fmap Just $
                Otel.createSpanWithoutCallStack (tracer ct) context (spanName (config ct) command statement) $
                    Otel.addAttributesToSpanArguments
                        (HashMap.fromList (shared <> attributes statement))
                        Otel.defaultSpanArguments{Otel.kind = Otel.Client}


-- | Wrap a command that sends and reads its own results.
tracedSync ::
    ConnectionTracer ->
    Command ->
    IO Statement ->
    IO (Maybe Pqi.Result) ->
    IO (Maybe Pqi.Result)
tracedSync ct command mkStatement act =
    bracket
        (startSpan ct command mkStatement)
        (traverse_ (`Otel.endSpan` Nothing))
        \mSpan -> do
            result <- act
            for_ mSpan (for_ result . annotateResult)
            pure result


-- | Wrap a command that only queues bytes for the server. The span stays open
-- until its results have been read off the connection.
--
-- Masked for the same reason as @getResult@: a command the backend accepted
-- and did not queue here is one the queue will never account for.
tracedSend :: ConnectionTracer -> Command -> IO Statement -> IO Bool -> IO Bool
tracedSend ct command mkStatement send = mask \restore -> do
    mSpan <- startSpan ct command mkStatement
    sent <- restore send `onException` failSpan mSpan "interrupted before the command was sent"
    if sent
        then True <$ enqueue ct mSpan
        else False <$ failSpan mSpan "the command could not be sent"


-- | Add a command to the queue of the unanswered.
enqueue :: ConnectionTracer -> Maybe Span -> IO ()
enqueue ct entry = do
    queued <- readIORef (pending ct)
    case queued of
        oldest Seq.:<| rest
            | Seq.length queued >= maxPendingCommands -> do
                abandonSpan oldest "no result was matched to this command"
                writeIORef (pending ct) (rest Seq.|> entry)
        _ -> writeIORef (pending ct) (queued Seq.|> entry)


-- | End the span of the oldest unanswered command: @getResult@ has returned
-- the 'Nothing' that closes it.
commandAnswered :: ConnectionTracer -> IO ()
commandAnswered ct =
    readIORef (pending ct) >>= \case
        Seq.Empty -> pure ()
        oldest Seq.:<| rest -> do
            writeIORef (pending ct) rest
            for_ oldest (`Otel.endSpan` Nothing)


-- | Record what a result says about the command it belongs to, which is the
-- oldest unanswered one.
resultReceived :: ConnectionTracer -> Pqi.Result -> IO ()
resultReceived ct result =
    readIORef (pending ct) >>= \case
        Just span_ Seq.:<| _ -> annotateResult span_ result
        _ -> pure ()


annotateResult :: Span -> Pqi.Result -> IO ()
annotateResult span_ result =
    Pqi.resultStatus result >>= \case
        Pqi.TuplesOk -> do
            rows <- Pqi.ntuples result
            Otel.addAttribute span_ "db.response.returned_rows" (fromIntegral rows :: Int)
        Pqi.CommandOk -> do
            affected <- Pqi.cmdTuples result
            for_ (affected >>= rowCount) (Otel.addAttribute span_ "db.postgresql.affected_rows")
        Pqi.FatalError -> recordFailure
        Pqi.BadResponse -> recordFailure
        Pqi.PipelineAbort -> recordFailure
        -- 'Pqi.NonfatalError' is libpq's notice-or-warning status, not a
        -- failed command. Marking it would report errors for statements
        -- Postgres was content with.
        _ -> pure ()
  where
    -- The SQLSTATE is the whole of the error report that is safe to send: it
    -- is a five-character code from the protocol, and every other field can
    -- carry the value that caused the failure.
    recordFailure = do
        sqlstate <- fmap decodeUtf8Lenient <$> Pqi.resultErrorField result Pqi.DiagSqlstate
        for_ sqlstate \code -> do
            Otel.addAttribute span_ "db.response.status_code" code
            Otel.addAttribute span_ "error.type" code
        Otel.setStatus span_ . Otel.Error $
            maybe
                "postgres reported an error"
                ("postgres reported an error, SQLSTATE " <>)
                sqlstate


-- | The rows a command affected, which @cmdTuples@ reports as a bare decimal.
-- Only a clean parse of the whole field counts: a command that touches no rows
-- reports an empty one.
rowCount :: ByteString -> Maybe Int
rowCount raw = case BS8.readInt raw of
    Just (rows, rest) | BS8.null rest -> Just rows
    _ -> Nothing


-- | End a span for a command that never reached the backend. Its outcome is
-- known, and it is a failure.
failSpan :: Maybe Span -> Text -> IO ()
failSpan mSpan reason =
    for_ mSpan \span_ -> do
        Otel.setStatus span_ (Otel.Error reason)
        Otel.endSpan span_ Nothing


-- | End a span whose command reached the backend but whose result we never
-- saw. Deliberately not an error: the statement most likely succeeded and we
-- simply stopped reading, which happens on every interrupted session. The
-- duration is a lower bound, hence the marker.
abandonSpan :: Maybe Span -> Text -> IO ()
abandonSpan mSpan reason =
    for_ mSpan \span_ -> do
        Otel.addAttribute span_ "db.postgresql.results_unread" reason
        Otel.endSpan span_ Nothing


-- | Close every span still waiting for a result. Called where the connection
-- can no longer answer them in order.
abandonPending :: ConnectionTracer -> IO ()
abandonPending ct = do
    queued <- atomicModifyIORef' (pending ct) (Seq.empty,)
    for_ queued (`abandonSpan` "the pipeline ended before the result arrived")


-- | Drop everything remembered about a connection that has been reset: its
-- prepared statements are gone from the server and its queued commands will
-- never be answered.
forget :: ConnectionTracer -> IO ()
forget ct = do
    abandonPending ct
    writeIORef (statements ct) mempty


-- * Attributes


-- | A statement referred to by the name it was prepared under. The SQL is
-- whatever the prepare on this connection recorded; a miss leaves the span
-- with the name alone, which still says which statement it was.
preparedStatement :: ConnectionTracer -> ByteString -> IO Statement
preparedStatement ct name = do
    remembered <- HashMap.lookup name <$> readIORef (statements ct)
    let statement = fromMaybe (Statement Nothing []) remembered
    pure
        statement
            { attributes =
                ("db.postgresql.prepared_statement", toAttribute (decodeUtf8Lenient name))
                    : attributes statement
            }


rememberStatement :: ConnectionTracer -> ByteString -> ByteString -> IO ()
rememberStatement ct name sql =
    modifyIORef' (statements ct) (HashMap.insert name (parseStatement (config ct) sql))


parseStatement :: Config -> ByteString -> Statement
parseStatement cfg sql =
    Statement
        { operation = op
        , attributes =
            ("db.query.text", text)
                : maybe [] (\o -> [("db.operation.name", toAttribute o)]) op
                <> if datadogCompat cfg
                    then [("db.statement", text), ("resource.name", text)]
                    else []
        }
  where
    text = toAttribute statement
    op = operationOf statement
    -- One line, so a backend that groups by the statement shows the query
    -- rather than the first line of its indentation.
    statement =
        cutTo (maxStatementLength cfg)
            . T.unwords
            . T.words
            $ decodeUtf8Lenient sql


-- | The leading keyword. Anything that does not start with a bare word is not
-- worth guessing at.
operationOf :: Text -> Maybe Text
operationOf statement = case T.words statement of
    word : _ | T.all Char.isAlpha word -> Just (T.toUpper word)
    _ -> Nothing


cutTo :: Int -> Text -> Text
cutTo limit text
    | T.length text > limit = T.take limit text <> "…"
    | otherwise = text


-- | Attributes of the connection, read once. Connections are used by one
-- thread at a time, so the read-then-write needs no more than an 'IORef'.
sharedAttributes :: ConnectionTracer -> IO [(Text, Attribute)]
sharedAttributes ct =
    readIORef (connectionAttributes ct) >>= \case
        Just cached -> pure cached
        Nothing -> do
            fresh <- readConnectionAttributes (config ct) (connection ct)
            writeIORef (connectionAttributes ct) (Just fresh)
            pure fresh


readConnectionAttributes :: Config -> Pqi.Connection -> IO [(Text, Attribute)]
readConnectionAttributes cfg pq = do
    name <- Pqi.db pq
    host <- Pqi.host pq
    port <- Pqi.port pq
    pure $
        ("db.system.name", toAttribute ("postgresql" :: Text))
            : [("span.type", toAttribute ("sql" :: Text)) | datadogCompat cfg]
            <> catMaybes
                [ ("db.namespace",) . toAttribute . decodeUtf8Lenient <$> name
                , ("server.address",) . toAttribute . decodeUtf8Lenient <$> host
                , ("server.port",) . toAttribute <$> (readMaybe . BS8.unpack =<< port :: Maybe Int)
                ]
