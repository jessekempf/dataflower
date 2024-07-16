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
    VertexReference,
    initDataflowState,
    Edge,
    Epoch (..),
    Timestamp (..),
    modifyIORef',
    atomically,
    inc,
    send,
    connect,
    vertex,
    input,
    quiesce
  )
where

import           Control.Arrow               ((>>>))
import           Control.Concurrent          (ThreadId, forkIO)
import           Control.Concurrent.STM      (STM, modifyTVar, modifyTVar',
                                              newTVar, readTVar, readTVarIO,
                                              retry, writeTVar, orElse, check, TMVar, newTMVarIO, takeTMVar, putTMVar, isEmptyTMVar)
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
import qualified Data.Semigroup as MinQ
import qualified Data.Map.Strict as Map
import Control.Monad.Fix (MonadFix)
import Control.Concurrent.STM.TSem (TSem, newTSem, waitTSem, signalTSem)

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
data Edge a = forall b. (Eq b, Show b) => Edge (a -> b) VertexID
newtype VertexReference a = VertexReference VertexID deriving Show

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
    dfsLastVertexID       :: VertexID
  }

initDataflowState :: DataflowState
initDataflowState =
  DataflowState{
    dfsVertices = empty,
    dfsLastVertexID = VertexID (-1)
  }

-- | `Dataflow` is the type of all dataflow operations.
--
-- @since 0.1.0.0
newtype Dataflow a = Dataflow {runDataflow :: ReaderT (IORef DataflowState) IO a}
  deriving (Functor, Applicative, Monad, MonadFix)

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

runStatefully :: TVar s -> (s -> Dataflow s) -> Dataflow ()
runStatefully stateRef action = atomically (readTVar stateRef) >>= action >>= atomically . writeTVar stateRef

data RunState =  Run | Stop deriving (Eq, Show)

data Vertex i = forall s. Vertex {
    vertexId :: VertexID,
    runState       :: TVar RunState,
    vertexStateRef :: TVar s,
    threadId       :: ThreadId,
    inputQueue :: TVar (MinPQ.MinPQueue Timestamp i),
    inProgress :: TSem,
    destinations :: TVar [VertexID],
    sourceCount :: TVar Natural,
    precursors :: TVar (Map Timestamp Natural)
  }

data VertexOperation i = Receive Timestamp i | Notify Timestamp deriving (Show, Eq)

instance Eq i => Ord (VertexOperation i) where
  compare (Receive tsl _) (Receive tsr _) = compare tsl tsr
  compare (Receive tsr _)  (Notify tsn) = if tsr == tsn then LT else compare tsr tsn
  compare (Notify tsn)  (Receive tsr _) = if tsn == tsr then GT else compare tsn tsr
  compare (Notify tsl) (Notify tsr) = compare tsl tsr

vertex :: forall i state. (Show i, Eq i) => state -> (Timestamp -> i -> state -> Dataflow state) -> (Timestamp -> state -> Dataflow state) -> Dataflow (VertexReference i)
vertex initialState onRecv onNotify = do
  runState <- Dataflow . liftIO $ newTVarIO Run

  vertexStateRef <- Dataflow . liftIO $ newTVarIO initialState
  inputQueue <- Dataflow . liftIO $ newTVarIO (MinPQ.empty :: MinPQ.MinPQueue Timestamp i)
  inProgress <- atomically $ newTSem 1
  destinations <- Dataflow . liftIO $ newTVarIO []
  sourceCount <- Dataflow . liftIO $ newTVarIO 0
  precursors <- Dataflow . liftIO $ newTVarIO Map.empty

  vertexId <- gets (dfsLastVertexID >>> inc)

  -- traceM $ printf "creating vertex %s" (show vertexId)

  vtx <- do
    threadId <- forkDataflow $ do
      loopCountRef <- Dataflow . liftIO $ Data.IORef.newIORef (0 :: Natural)
      -- traceM "\n"
      -- traceM $ printf "%s started" (show vertexId)

      while runState (== Run) $ do
        loopCount <- readIORef loopCountRef

        -- traceM $ printf "%s: Loop %d started!" (show vertexId) loopCount

        op <- atomically $ do
            precursorTable <- readTVar precursors
            opQueue <- readTVar inputQueue
            (
              case MinPQ.minViewWithKey opQueue of
                Nothing -> do
                  -- traceM $ printf "%s: loop %d retrying on operationQueue" (show vertexId) loopCount
                  retry
                Just ((timestamp, item), opQueue') -> do
                  -- traceM $ printf "%s: loop continuing with %s / %s" (show vertexId) (show timestamp) (show item)
                  writeTVar inputQueue opQueue'
                  waitTSem inProgress
                  return $ Receive timestamp item
              ) `orElse` (
              case Data.Map.Strict.minViewWithKey precursorTable of
                Just ((timestamp, 0), precursorQueue') -> do
                  -- traceM $ printf "%s: loop continuing by retiring %s" (show vertexId) (show timestamp)
                  writeTVar precursors precursorQueue'
                  waitTSem inProgress
                  return $ Notify timestamp
                _ -> do
                  -- traceM $ printf "%s: loop %d retrying on precurosrs" (show vertexId) loopCount
                  retry
              )


        -- traceShowM op

        case op of
          Receive timestamp item -> do
            -- traceM $ printf "%s: running onSend for %s / %s" (show vertexId) (show timestamp) (show item)
            runStatefully vertexStateRef $ onRecv timestamp item
            atomically $ signalTSem inProgress

          Notify timestamp -> do
            destinationVertexIDs <- atomically (readTVar destinations)

            -- traceM $ printf "%s: running onNotify for %s" (show vertexId) (show timestamp)
            runStatefully vertexStateRef $ onNotify timestamp

            forM_ destinationVertexIDs (decrementPrecursors timestamp)
            atomically $ signalTSem inProgress

        -- traceM $ printf "%s: Loop %d completed!" (show vertexId) loopCount
        modifyIORef' loopCountRef (+ 1)

    return Vertex{..}

  -- traceM $ printf "created vertex %s" (show vertexId)

  vertices <- gets (dfsVertices >>> (`snoc` unsafeCoerce vtx))
  modify (\s -> s { dfsVertices = vertices, dfsLastVertexID = vertexId })

  -- traceM $ printf "registered vertex %s" (show vertexId)

  return (VertexReference vertexId)

    where
      while :: TVar a -> (a -> Bool) -> Dataflow () -> Dataflow ()
      while stateVar predicate action = do
        state <- Dataflow . liftIO $ readTVarIO stateVar
        when (predicate state) $ do
          action
          while stateVar predicate action

      decrementPrecursors :: Timestamp -> VertexID -> Dataflow ()
      decrementPrecursors timestamp (VertexID vid) = do
        Vertex{..} <- gets (dfsVertices >>> (! vid) >>> unsafeCoerce)

        atomically $ modifyTVar' precursors (Data.Map.Strict.adjust (\x -> x - 1) timestamp)



connect :: (Eq a, Show a) => VertexReference s -> VertexReference a -> Dataflow (Edge a)
connect source destination = do
  -- traceM $ printf "connecting vertex %s -> %s: started" (show source) (show destination)
  (vtxSource :: Vertex s) <- lookupVertex source
  (vtxDestination :: Vertex a) <- lookupVertex destination

  -- traceM $ printf "connecting vertex %s -> %s: looked up vertices" (show source) (show destination)

  atomically $ do
    modifyTVar' (destinations vtxSource) (vertexId vtxDestination :)
    modifyTVar' (sourceCount vtxDestination) (+1)

  -- traceM $ printf "connecting vertex %s -> %s: connected inputs and outputs" (show source) (show destination)

  return (Edge id (vertexId vtxDestination))

lookupVertex :: VertexReference i -> Dataflow (Vertex i)
lookupVertex (VertexReference (VertexID vid)) = gets (dfsVertices >>> (! vid) >>> unsafeCoerce)

send :: Show i => Edge i -> Timestamp -> i -> Dataflow ()
send (Edge f vid@(VertexID vindex)) timestamp i = do
  -- traceM $ printf "sending %s / %s to %s" (show timestamp) (show i) (show vid)
  (vtx :: Vertex b) <- gets (dfsVertices >>> (! vindex) >>> unsafeCoerce)
  -- traceM $ printf "send to %s: loaded vertex" (show vid)

  atomically $ do
    modifyTVar' (inputQueue vtx) (MinPQ.insert timestamp (f i))
    -- traceM $ printf "send to %s: updated vertex input queue" (show vid)
    -- iq <- readTVar (inputQueue vtx)
    -- traceM $ printf "send to %s %s: input queue: %s" (show vid) (show timestamp) (show iq)

input :: (Eq i, Show i, Show (t i), Traversable t) => VertexReference i -> Timestamp -> t i -> Dataflow ()
input vertexRef timestamp items = do
  -- traceM (show timestamp ++ " -> looking up: " ++ show vertexRef)

  destination <- lookupVertex vertexRef

  -- traceM (show timestamp ++ " -> inputting: " ++ show items)
  forM_ items $ \item -> do
    atomically $ do
      modifyTVar' (inputQueue destination) (MinPQ.insert timestamp item)
      -- iq <- readTVar (inputQueue destination)
      -- traceM $ printf "input to %s %s: input queue: %s" (show $ vertexId destination) (show timestamp) (show iq)

  allVertices <- fmap unsafeCoerce <$> gets dfsVertices

  atomically $ do
    forM_ allVertices $ \Vertex{..} -> do
      precursorTable <- readTVar precursors

      unless (timestamp `Data.Map.Strict.member` precursorTable) $ do
        numSources <- readTVar sourceCount
        -- traceM $ printf "%s: adding %s to the precursor table with count %d" (show vertexId) (show timestamp) numSources
        modifyTVar' precursors (Data.Map.Strict.insert timestamp numSources)

  -- traceM (show timestamp ++ " -> inputted: " ++ show items)

quiesce :: Dataflow ()
quiesce = do
  allVertices <- fmap unsafeCoerce <$> gets dfsVertices

  atomically $ do
    forM_ allVertices $ \Vertex{..} -> do
      synchronizeTSem inProgress

      precursorTable <- readTVar precursors
      inputs <- readTVar inputQueue

      check (MinPQ.null inputs)
      check (Data.Map.Strict.null precursorTable)
  where
    synchronizeTSem tsem = waitTSem tsem >> signalTSem tsem