{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Use readTVarIO" #-}
{-# LANGUAGE InstanceSigs               #-}


module Dataflow.Primitives
  ( Dataflow (..),
    Node(..),
    DataflowState,
    Vertex (..),
    VertexReference,
    initDataflowState,
    Edge,
    Epoch (..),
    Timestamp (..),
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
                                              retry, stateTVar, writeTQueue,
                                              writeTVar)
import qualified Control.Concurrent.STM
import           Control.Concurrent.STM.TVar (TVar, newTVarIO)
import           Control.Monad               (forM_, unless, when)
import           Control.Monad.Fix           (MonadFix)
import           Control.Monad.IO.Class      (liftIO)
import           Control.Monad.Reader        (MonadReader (ask),
                                              MonadTrans (lift),
                                              ReaderT (runReaderT))
import           Control.Monad.State         (MonadState (state), gets, modify)
import           Data.Functor.Contravariant  (Contravariant (contramap))
import           Data.Hashable               (Hashable (..))
import           Data.Map.Strict             (Map)
import qualified Data.Map.Strict
import qualified Data.Map.Strict             as Map
import           Data.Vector                 (Vector, empty, snoc, unsafeIndex,
                                              (!))
import           GHC.Exts                    (Any, inline)
import           Numeric.Natural             (Natural)
import           Prelude                     hiding (min, (<>))
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
data Edge a = (Eq a, Show a) => Direct {-# UNPACK #-} VertexID | forall b. (Eq b, Show b) => Contra {-# UNPACK #-} (a -> b) {-# UNPACK #-} VertexID
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
newtype Dataflow a = Dataflow {runDataflow :: ReaderT (TVar DataflowState) IO a}
  deriving (Functor, Applicative, Monad, MonadFix)

instance MonadState DataflowState Dataflow where
  state :: (DataflowState -> (a, DataflowState)) -> Dataflow a
  state action = do
    dataflowStateRef <- Dataflow ask
    atomically $ stateTVar dataflowStateRef action

newtype Node a = Node (ReaderT (TVar DataflowState) STM a)
  deriving (Functor, Applicative, Monad, MonadFix)

instance MonadState DataflowState Node where
  state :: (DataflowState -> (a, DataflowState)) -> Node a
  state action = do
    dataflowStateRef <- Node ask
    Node . lift $ stateTVar dataflowStateRef action


forkDataflow :: Dataflow () -> Dataflow ThreadId
forkDataflow action = Dataflow $ do
  stateRef <- ask

  liftIO . forkIO $
    runReaderT (runDataflow action) stateRef

{-# INLINE atomically #-}
atomically :: STM a -> Dataflow a
atomically = Dataflow . liftIO . Control.Concurrent.STM.atomically

{-# INLINE runStatefully #-}
runStatefully :: TVar s -> (s -> Node s) -> Node ()
runStatefully stateRef action = Node (lift $ readTVar stateRef) >>= action >>= Node . lift . writeTVar stateRef

{-# INLINE runNodeAtomically #-}
runNodeAtomically :: Node a -> Dataflow a
runNodeAtomically (Node stmActions) = do
  stateRef <- Dataflow ask
  atomically (runReaderT stmActions stateRef)

{-# INLINE (<>) #-}
(<>) :: Node a -> Node a -> Node a
(Node first) <> (Node second) = Node $ do
  stateRef <- ask

  lift (runReaderT first stateRef `orElse` runReaderT second stateRef)

data RunState =  Run | Stop deriving (Eq, Show)

data Vertex i = forall s. Vertex {
    vertexId       :: VertexID,
    runState       :: TVar RunState,
    vertexStateRef :: TVar s,
    threadId       :: ThreadId,
    inputQueue     :: TQueue (Timestamp, i),
    destinations   :: TVar [VertexID],
    sourceCount    :: TVar Natural,
    precursors     :: TVar (Map Timestamp Natural)
  }

vertex :: forall i state. (Show i, Eq i) => state -> (Timestamp -> i -> state -> Node state) -> (Timestamp -> state -> Node state) -> Dataflow (VertexReference i)
vertex initialState onRecv onNotify = do
  runState <- Dataflow . liftIO $ newTVarIO Run

  vertexStateRef <- Dataflow . liftIO $ newTVarIO initialState
  inputQueue <- Dataflow . liftIO $ newTQueueIO
  destinations <- Dataflow . liftIO $ newTVarIO []
  sourceCount <- Dataflow . liftIO $ newTVarIO 0
  precursors <- Dataflow . liftIO $ newTVarIO Map.empty

  vertexId <- gets (dfsLastVertexID >>> inc)

  vtx <- do
    threadId <- forkDataflow $ do
      while runState (== Run) $ do
        runNodeAtomically $ (do
            (ts, i) <- Node . lift $ readTQueue inputQueue
            runStatefully vertexStateRef $ onRecv ts i
          ) <> (do
            timestamp <- Node . lift $ do
              precursorTable <- readTVar precursors
              case Data.Map.Strict.minViewWithKey precursorTable of
                Just ((timestamp, 0), precursorQueue') -> do
                  writeTVar precursors precursorQueue'
                  return timestamp
                _ -> retry
            destinationVertexIDs <- Node . lift $ readTVar destinations
            runStatefully vertexStateRef $ onNotify timestamp
            forM_ destinationVertexIDs (decrementPrecursors timestamp)
          )

    return Vertex{..}

  vertices <- gets (dfsVertices >>> (`snoc` unsafeCoerce vtx))
  modify (\s -> s { dfsVertices = vertices, dfsLastVertexID = vertexId })

  return (VertexReference vertexId)

    where
      while :: TVar a -> (a -> Bool) -> Dataflow () -> Dataflow ()
      while stateVar predicate action = do
        condition <- predicate <$> (Dataflow . liftIO $ readTVarIO stateVar)
        when condition $ do
          action
          while stateVar predicate action

      decrementPrecursors :: Timestamp -> VertexID -> Node ()
      decrementPrecursors timestamp (VertexID vid) = do
        Vertex{..} <- gets (dfsVertices >>> (! vid) >>> unsafeCoerce)
        Node . lift $ modifyTVar' precursors (Data.Map.Strict.adjust (\x -> x - 1) timestamp)



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
send :: Show i => Edge i -> Timestamp -> i -> Node ()
send (Direct (VertexID vindex)) timestamp i = do
  vertices <- gets dfsVertices
  let vtx = inline (unsafeCoerce (vertices `unsafeIndex` vindex))

  Node . lift $ writeTQueue (inputQueue vtx) (timestamp, i)
send (Contra f (VertexID vindex)) timestamp i = do
  vertices <- gets dfsVertices
  let vtx = inline (unsafeCoerce (vertices `unsafeIndex` vindex))

  Node . lift $ writeTQueue (inputQueue vtx) (timestamp, f i)

{-# INLINE input #-}
input :: (Eq i, Show i, Show (t i), Traversable t) => VertexReference i -> Timestamp -> t i -> Dataflow ()
input vertexRef timestamp items = do
  destination <- lookupVertex vertexRef

  forM_ items $ \item -> do
    atomically $ do
      writeTQueue (inputQueue destination) (timestamp, item)

  allVertices <- gets (fmap unsafeCoerce . dfsVertices)

  atomically $ do
    forM_ allVertices $ \Vertex{..} -> do
      precursorTable <- readTVar precursors

      unless (timestamp `Data.Map.Strict.member` precursorTable) $ do
        numSources <- readTVar sourceCount
        modifyTVar' precursors (Data.Map.Strict.insert timestamp numSources)

{-# INLINE quiesce #-}
quiesce :: Dataflow ()
quiesce = do
  allVertices <- gets (fmap unsafeCoerce . dfsVertices)

  atomically $ do
    forM_ allVertices $ \Vertex{..} -> do

      check =<< isEmptyTQueue inputQueue
      check . Data.Map.Strict.null =<< readTVar precursors
