{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
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
import           Control.Concurrent.STM      (STM, TQueue, check, isEmptyTQueue,
                                              modifyTVar', newTQueueIO, orElse,
                                              readTQueue, readTVar, readTVarIO,
                                              retry, writeTQueue, writeTVar)
import qualified Control.Concurrent.STM
import           Control.Concurrent.STM.TSem (TSem, newTSem, signalTSem,
                                              waitTSem)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO)
import           Control.Monad               (forM_, unless, when)
import           Control.Monad.Fix           (MonadFix)
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
import qualified Data.Map.Strict             as Map
import           Data.Vector                 (Vector, empty, snoc, unsafeIndex,
                                              (!))
import           GHC.Exts                    (Any, inline)
import           Numeric.Natural             (Natural)
import           Prelude                     hiding (min)
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
data Edge a = (Eq a, Show a) => Direct {-# UNPACK #-} VertexID | forall b. (Eq b, Show b) => Contra (a -> b) VertexID
newtype VertexReference a = VertexReference VertexID deriving Show

instance Contravariant Edge where
  contramap f (Direct vid)   = Contra f vid
  contramap f (Contra g vid) = Contra (g . f) vid


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
  { dfsVertices     :: Vector Any,
    dfsLastVertexID :: VertexID
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

modifyIORef' :: IORef a -> (a -> a) -> Dataflow ()
modifyIORef' ref = Dataflow . liftIO . Data.IORef.modifyIORef' ref

runStatefully :: TVar s -> (s -> Dataflow s) -> Dataflow ()
runStatefully stateRef action = (Dataflow . liftIO $ readTVarIO stateRef) >>= action >>= atomically . writeTVar stateRef

data RunState =  Run | Stop deriving (Eq, Show)

data Vertex i = forall s. Vertex {
    vertexId       :: VertexID,
    runState       :: TVar RunState,
    vertexStateRef :: TVar s,
    threadId       :: ThreadId,
    inputQueue     :: TQueue (Timestamp, i),
    inProgress     :: TSem,
    destinations   :: TVar [VertexID],
    sourceCount    :: TVar Natural,
    precursors     :: TVar (Map Timestamp Natural)
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
  inputQueue <- Dataflow . liftIO $ newTQueueIO
  inProgress <- atomically $ newTSem 1
  destinations <- Dataflow . liftIO $ newTVarIO []
  sourceCount <- Dataflow . liftIO $ newTVarIO 0
  precursors <- Dataflow . liftIO $ newTVarIO Map.empty

  vertexId <- gets (dfsLastVertexID >>> inc)

  vtx <- do
    threadId <- forkDataflow $ do
      while runState (== Run) $ do
        op <- atomically $
            (do
                (ts, i) <- readTQueue inputQueue
                waitTSem inProgress
                return $ Receive ts i
              ) `orElse`
            (do
              precursorTable <- readTVar precursors
              case Data.Map.Strict.minViewWithKey precursorTable of
                Just ((timestamp, 0), precursorQueue') -> do
                  writeTVar precursors precursorQueue'
                  waitTSem inProgress
                  return $ Notify timestamp
                _ -> retry
              )

        case op of
          Receive timestamp item -> do
            runStatefully vertexStateRef $ onRecv timestamp item
            atomically $ signalTSem inProgress

          Notify timestamp -> do
            destinationVertexIDs <- atomically (readTVar destinations)
            runStatefully vertexStateRef $ onNotify timestamp

            forM_ destinationVertexIDs (decrementPrecursors timestamp)
            atomically $ signalTSem inProgress

    return Vertex{..}

  vertices <- gets (dfsVertices >>> (`snoc` unsafeCoerce vtx))
  modify (\s -> s { dfsVertices = vertices, dfsLastVertexID = vertexId })

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
  (vtxSource :: Vertex s) <- lookupVertex source
  (vtxDestination :: Vertex a) <- lookupVertex destination

  atomically $ do
    modifyTVar' (destinations vtxSource) (vertexId vtxDestination :)
    modifyTVar' (sourceCount vtxDestination) (+1)

  return (Direct (vertexId vtxDestination))

{-# INLINE lookupVertex #-}
lookupVertex :: VertexReference i -> Dataflow (Vertex i)
lookupVertex (VertexReference (VertexID vid)) = do
  vertices <- gets dfsVertices
  return $ unsafeCoerce (vertices `unsafeIndex` vid)

{-# INLINE send #-}
send :: Show i => Edge i -> Timestamp -> i -> Dataflow ()
send (Direct (VertexID vindex)) timestamp i = do
  vertices <- gets dfsVertices
  let vtx = inline (unsafeCoerce (vertices `unsafeIndex` vindex))

  atomically $ do
    writeTQueue (inputQueue vtx) (timestamp, i)

send (Contra f (VertexID vindex)) timestamp i = do
  vertices <- gets dfsVertices
  let vtx = inline (unsafeCoerce (vertices `unsafeIndex` vindex))

  atomically $ do
    writeTQueue (inputQueue vtx) (timestamp, f i)

{-# INLINE input #-}
input :: (Eq i, Show i, Show (t i), Traversable t) => VertexReference i -> Timestamp -> t i -> Dataflow ()
input vertexRef timestamp items = do
  destination <- lookupVertex vertexRef

  forM_ items $ \item -> do
    atomically $ do
      writeTQueue (inputQueue destination) (timestamp, item)

  allVertices <- fmap unsafeCoerce <$> gets dfsVertices

  atomically $ do
    forM_ allVertices $ \Vertex{..} -> do
      precursorTable <- readTVar precursors

      unless (timestamp `Data.Map.Strict.member` precursorTable) $ do
        numSources <- readTVar sourceCount
        modifyTVar' precursors (Data.Map.Strict.insert timestamp numSources)

{-# INLINE quiesce #-}
quiesce :: Dataflow ()
quiesce = do
  allVertices <- fmap unsafeCoerce <$> gets dfsVertices

  atomically $ do
    forM_ allVertices $ \Vertex{..} -> do
      synchronizeTSem inProgress

      check =<< isEmptyTQueue inputQueue
      check . Data.Map.Strict.null =<< readTVar precursors
  where
    synchronizeTSem tsem = waitTSem tsem >> signalTSem tsem
