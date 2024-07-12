{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use readTVarIO" #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Dataflow.Primitives
  ( Dataflow (..),
    DataflowState,
    Vertex (..),
    initDataflowState,
    Edge,
    Epoch (..),
    Timestamp (..),
    modifyIORef',
    atomically,
    inc,
    send,
    -- drain,
    vertex,
    input,
  )
where

import           Control.Arrow               ((>>>))
import           Control.Concurrent          (ThreadId, forkIO)
import           Control.Concurrent.STM      (STM, modifyTVar, modifyTVar',
                                              newTVar, readTVar, readTVarIO,
                                              retry, writeTVar)
import qualified Control.Concurrent.STM
import           Control.Concurrent.STM.TVar (TVar, newTVarIO)
import           Control.Monad               (forM_, unless, when)
import           Control.Monad.IO.Class      (liftIO)
import           Control.Monad.Reader        (MonadReader (ask),
                                              ReaderT (runReaderT))
import           Data.Functor                ((<&>))
import           Data.Functor.Contravariant  (Contravariant (contramap))
import           Data.Hashable               (Hashable (..))
import           Data.IORef                  (IORef)
import qualified Data.IORef
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict
import qualified Data.PQueue.Min             as MinQ
import qualified Data.PQueue.Prio.Min        as MinPQ
import           Data.Set                    (Set)
import qualified Data.Set
import           Data.Vector                 (Vector, empty, length, snoc,
                                              unsafeIndex, (!))
import           Data.Void                   (Void)
import           Debug.Trace                 (traceM, traceShowM)
import           GHC.Exts                    (Any)
import           Numeric.Natural             (Natural)
import           Prelude                     hiding (min)
import           Text.Printf
import           Unsafe.Coerce               (unsafeCoerce)

newtype VertexID = VertexID Int deriving (Eq, Ord, Show)

newtype StateID = StateID Int deriving (Eq, Ord, Show)

newtype Epoch = Epoch Natural deriving (Eq, Ord, Hashable, Show)

-- | 'Timestamp's represent instants in the causal timeline.
--
-- @since 0.1.0.0
newtype Timestamp = Timestamp Epoch deriving (Eq, Ord, Show)

instance Hashable Timestamp where
  hashWithSalt salt (Timestamp epoch) = hashWithSalt salt epoch

-- | An 'Edge' is a typed reference to a computational vertex that
-- takes 'a's as its input.
--
-- @since 0.1.0.0
data Edge a = forall b. Show b => Edge (a -> b) VertexID

instance Contravariant Edge where
  contramap f (Edge g vid) = Edge (g . f) vid


-- | Class of entities that can be incremented by one.
class Incrementable a where
  inc :: a -> a

instance Incrementable VertexID where
  inc (VertexID n) = VertexID (n + 1)

instance Incrementable StateID where
  inc (StateID n) = StateID (n + 1)

instance Incrementable Epoch where
  inc (Epoch n) = Epoch (n + 1)

data DataflowState = DataflowState
  { dfsVertices           :: Vector Any,
    dfsLastVertexID       :: VertexID,
    dfsTimestampCounts    :: TVar (Map Timestamp Natural),
    dfsTimestampProducers :: TVar (Map Timestamp (Set VertexID))
  }

initDataflowState :: STM DataflowState
initDataflowState = do
  dfsTimestampCounts <- newTVar Data.Map.Strict.empty
  dfsTimestampProducers <- newTVar Data.Map.Strict.empty

  return DataflowState{..}

  where
    dfsVertices = empty
    dfsLastVertexID = VertexID (-1)

-- | `Dataflow` is the type of all dataflow operations.
--
-- @since 0.1.0.0
newtype Dataflow a = Dataflow {runDataflow :: ReaderT (IORef DataflowState) IO a}
  deriving (Functor, Applicative, Monad)

gets :: (DataflowState -> a) -> Dataflow a
gets f = Dataflow ask >>= readIORef <&> f

modify :: (DataflowState -> DataflowState) -> Dataflow ()
modify f = Dataflow ask >>= \r -> modifyIORef' r f

forkDataflow :: Dataflow () -> Dataflow ThreadId
forkDataflow action = Dataflow $ do
  stateRef <- ask

  liftIO . forkIO $
    runReaderT (runDataflow action) stateRef

atomically :: STM a -> Dataflow a
atomically = Dataflow . liftIO . Control.Concurrent.STM.atomically

readIORef :: IORef a -> Dataflow a
readIORef = Dataflow . liftIO . Data.IORef.readIORef

writeIORef :: IORef a -> a -> Dataflow ()
writeIORef ref = Dataflow . liftIO . Data.IORef.writeIORef ref

modifyIORef' :: IORef a -> (a -> a) -> Dataflow ()
modifyIORef' ref = Dataflow . liftIO . Data.IORef.modifyIORef' ref

runStatefully :: IORef s -> (s -> Dataflow s) -> Dataflow ()
runStatefully stateRef action = readIORef stateRef >>= action >>= writeIORef stateRef

data RunState =  Run | Stop deriving (Eq, Show)

data Vertex i = forall s. Vertex {
    runState       :: TVar RunState,
    vertexStateRef :: IORef s,
    threadId       :: ThreadId,
    inputQueue     :: TVar (MinPQ.MinPQueue Timestamp i)--,
    -- notifyQueue    :: TVar (MinQ.MinQueue Timestamp)
  }

vertex :: Show i => state -> (Timestamp -> i -> state -> Dataflow state) -> (Timestamp -> state -> Dataflow state) -> Dataflow (Edge i)
vertex initialState onSend onNotify = do
  timestampCounters <- gets dfsTimestampCounts
  timestampProducers <- gets dfsTimestampProducers

  runState <- Dataflow . liftIO $ newTVarIO Run
  vertexStateRef <- Dataflow . liftIO $ Data.IORef.newIORef initialState
  inputQueue <- Dataflow . liftIO $ newTVarIO MinPQ.empty
  notifyQueue <- Dataflow . liftIO $ newTVarIO MinQ.empty

  vertexId <- gets (dfsLastVertexID >>> inc)

  vtx <- do
    threadId <- forkDataflow $ do
      loopCountRef <- Dataflow . liftIO $ Data.IORef.newIORef (0 :: Natural)
      -- traceM "\n"
      -- traceM $ printf "%s started" (show vertexId)

      while runState (== Run) $ do
        loopCount <- readIORef loopCountRef

        -- traceM $ printf "%s: Loop %d started!" (show vertexId) loopCount

        (mbWorkUnit, mbFinishedTimestamp) <- atomically $ do
          iq <- readTVar inputQueue
          nq <- readTVar notifyQueue

          if MinPQ.null iq && MinQ.null nq then do
            -- traceM $ printf "%s: loop %d retrying" (show vertexId) loopCount
            retry
          else
            return ()
            -- traceM $ printf "%s: loop continuing" (show vertexId)

          -- traceM "inputQueue ==v"
          -- traceShowM iq
          -- traceM "inputQueue ==^"

          case (MinPQ.minViewWithKey iq, MinQ.minView nq) of
            (Nothing, Nothing) ->
              return (Nothing, Nothing)
            (Nothing, Just (finishedTimestamp, nq')) -> do
              writeTVar notifyQueue nq'
              return (Nothing, Just finishedTimestamp)
            (Just (workUnit, iq'), Nothing) -> do
              writeTVar inputQueue iq'
              return (Just workUnit, Nothing)
            (Just ((timestamp, i), iq'), Just (finishedTimestamp, nq')) ->
              if timestamp > finishedTimestamp then do
                writeTVar inputQueue iq'
                writeTVar notifyQueue nq'
                return (Just (timestamp, i), Just finishedTimestamp)
              else do
                writeTVar inputQueue iq'
                return (Just (timestamp, i), Nothing)

        traceM $ printf "%s: %s" (show vertexId) (show (mbWorkUnit, mbFinishedTimestamp))

        case (mbWorkUnit, mbFinishedTimestamp) of
          (Nothing, Nothing) -> return ()
          (Just (timestamp, item), Nothing) -> do
            traceM $ printf "%s: running onSend for %s / %s" (show vertexId) (show timestamp) (show item)
            runStatefully vertexStateRef $ onSend timestamp item
          (Nothing, Just finishedTimestamp) -> do
            traceM $ printf "%s: running onNotify for %s" (show vertexId) (show finishedTimestamp)
            runStatefully vertexStateRef $ onNotify finishedTimestamp
          (Just (timestamp, item), Just finishedTimestamp) -> do
            traceM $ printf "%s: running onNotify for %s" (show vertexId) (show finishedTimestamp)
            runStatefully vertexStateRef $ onNotify finishedTimestamp
            traceM $ printf "%s: running onSend for %s / %s" (show vertexId) (show timestamp) (show item)
            runStatefully vertexStateRef $ onSend timestamp item

        -- traceM $ printf "%s: Loop %d completed!" (show vertexId) loopCount
        modifyIORef' loopCountRef (+ 1)

    return Vertex{..}

  vertices <- gets (dfsVertices >>> (`snoc` unsafeCoerce vtx))
  modify (\s -> s { dfsVertices = vertices, dfsLastVertexID = vertexId })

  return (Edge id vertexId)

    where
      while :: TVar a -> (a -> Bool) -> Dataflow () -> Dataflow ()
      while stateVar predicate action = do
        state <- Dataflow . liftIO $ readTVarIO stateVar
        when (predicate state) $ do
          action
          while stateVar predicate action

send :: Show i => Edge i -> Timestamp -> i -> Dataflow ()
send (Edge f vid@(VertexID vindex)) timestamp i = do
  -- traceM $ printf "sending %s to %s" (show i) (show vid)
  (vtx :: Vertex b) <- gets (dfsVertices >>> (! vindex) >>> unsafeCoerce)
  timestampCounters <- gets dfsTimestampCounts
  -- traceM $ printf "send to %s: loaded vertex" (show vid)

  atomically $ do
    modifyTVar' (inputQueue vtx) (MinPQ.insert timestamp (f i))
    modifyTVar' timestampCounters (Data.Map.Strict.adjust (+1) timestamp)
    -- traceM $ printf "send to %s: updated vertex input queue" (show vid)
    -- iq <- readTVar (inputQueue vtx)
    -- traceM $ printf "send to %s: input queue: %s" (show vid) (show iq)

-- drain :: Dataflow()
-- drain = do
--   (vertices :: Vector (Vertex Void)) <- gets (dfsVertices >>> unsafeCoerce)

--   forM_ vertices $ \vtx ->
--     atomically $ do
--       nq <- readTVar (notifyQueue vtx)
--       unless (MinQ.null nq) retry

input :: (Show i, Show (t i), Traversable t) => Edge i -> Timestamp -> t i -> Dataflow ()
input edge timestamp items = do
  timestampCounters <- gets dfsTimestampCounts
  timestampProducers <- gets dfsTimestampProducers
  (VertexID maxVertexIdx) <- gets dfsLastVertexID

  let vertexIds = Data.Set.fromList $ map VertexID [0..maxVertexIdx]

  atomically $ do
    modifyTVar' timestampCounters (Data.Map.Strict.insert timestamp 0)
    modifyTVar' timestampProducers (Data.Map.Strict.insert timestamp vertexIds)

  traceM (show timestamp ++ " -> inputting: " ++ show items)
  forM_ items $ send edge timestamp
  traceM (show timestamp ++ " -> inputted: " ++ show items)
