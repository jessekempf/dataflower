{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase #-}

module Dataflow.Primitives
  ( Graph (..),
    Node(..),
    DataflowGraph(..),
    Vertex,
    VertexID(..),
    VertexDef(..),
    initDataflowState,
    Edge,
    Input(..),
    Epoch (..),
    Timestamp (..),
    inc,
    send,
    vertex,
    using,
    output,
    inputVertex,
    runNode,
    runStatefully,
    (<>),
    -- quiesce
  )
where

import           Control.Arrow               ((>>>))
import           Control.Concurrent.STM      (STM, TQueue, newTQueueIO, orElse,
                                              readTVar, writeTQueue, writeTVar)
import           Control.Concurrent.STM.TVar (TVar, newTVarIO)
import           Control.Monad.Fix           (MonadFix)
import           Control.Monad.IO.Class      (liftIO)
import           Control.Monad.Reader        (MonadReader (ask),
                                              MonadTrans (lift),
                                              ReaderT (runReaderT))
import           Control.Monad.State         (StateT, gets, modify)
import           Data.Functor.Contravariant  (Contravariant (contramap))
import           Data.Hashable               (Hashable (..))
import           Data.Vector                 (Vector, empty, snoc, (!), (//))
import           GHC.Exts                    (Any)
import           Numeric.Natural             (Natural)
import           Prelude                     hiding (min, (<>))
import           Unsafe.Coerce               (unsafeCoerce)
import qualified Data.Map.Strict
import Debug.Trace (traceM)
import Text.Printf (printf)

newtype VertexID = VertexID Int deriving (Eq, Ord, Show)

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
data Edge a = Direct {-# UNPACK #-} (TQueue (Timestamp, a)) | forall b. Mapped {-# UNPACK #-} (a -> b) {-# UNPACK #-} (TQueue (Timestamp, b))

newtype Vertex a = Vertex VertexID deriving Show

newtype Input a = Input (TQueue (Timestamp, a))
instance Contravariant Edge where
  contramap f (Direct inputQueue)   = Mapped f inputQueue
  contramap f (Mapped g inputQueue) = Mapped (g . f) inputQueue


-- | Class of entities that can be incremented by one.
class Incrementable a where
  inc :: a -> a

instance Incrementable VertexID where
  inc (VertexID n) = VertexID (n + 1)

instance Incrementable Epoch where
  inc (Epoch n) = Epoch (n + 1)

newtype DataflowGraph = DataflowGraph { dfsVertices     :: Vector (VertexDef Any) }

initDataflowState :: DataflowGraph
initDataflowState = DataflowGraph{ dfsVertices = empty }

-- | `Dataflow` is the type of all dataflow operations.
--
-- @since 0.1.0.0
newtype Graph a = Graph {runDataflow :: StateT DataflowGraph IO a}
  deriving (Functor, Applicative, Monad)

getVertexDef :: Vertex i -> Graph (VertexDef i)
getVertexDef (Vertex (VertexID index)) = Graph $ gets (dfsVertices >>> (! index) >>> unsafeCoerce)

updateVertexDef :: Vertex i -> (VertexDef i -> VertexDef i) -> Graph ()
updateVertexDef (Vertex (VertexID index)) mutator = Graph $ do
  modify (\DataflowGraph{..} ->
      DataflowGraph {
        dfsVertices = dfsVertices // [(index, unsafeCoerce (mutator (unsafeCoerce (dfsVertices ! index))))],
        ..
      }
    )
newtype Node a = Node (ReaderT DataflowGraph STM a)
  deriving (Functor, Applicative, Monad)


runNode :: DataflowGraph -> Node a -> STM a
runNode graph (Node actions) = runReaderT actions graph

{-# INLINE runStatefully #-}
runStatefully :: TVar s -> (s -> Node s) -> Node ()
runStatefully stateRef action = Node (lift $ readTVar stateRef) >>= action >>= Node . lift . writeTVar stateRef

{-# INLINE (<>) #-}
(<>) :: Node a -> Node a -> Node a
(Node first) <> (Node second) = Node $ do
  stateRef <- ask

  lift (runReaderT first stateRef `orElse` runReaderT second stateRef)
data VertexDef input = forall state. VertexDef {
  vertexDefId         :: VertexID,
  vertexDefStateRef   :: TVar state,
  vertexDefInputQueue :: TQueue (Timestamp, input),
  vertexDefInputs     :: [VertexID],
  vertexDefOutputs    :: [VertexID],
  vertexDefOnSend     :: Timestamp -> input -> state -> Node state,
  vertexDefOnNotify   :: Timestamp -> state -> Node state
}

vertex :: state -> (Timestamp -> input -> state -> Node state) -> (Timestamp -> state -> Node state) -> Graph (Vertex input)
vertex initialState vertexDefOnSend vertexDefOnNotify = Graph $ do
  vertexId <- gets (dfsVertices >>> length >>> VertexID)

  vertexDefStateRef <- liftIO $ newTVarIO initialState
  vertexDefInputQueue <- liftIO newTQueueIO

  let vertexDef = VertexDef {
    vertexDefId = vertexId,
    vertexDefInputs = [],
    vertexDefOutputs = [],
    ..
  }

  modify (\DataflowGraph{..} -> DataflowGraph{ dfsVertices = dfsVertices `snoc` unsafeCoerce vertexDef })

  return (Vertex vertexId)

using :: forall output input. Vertex output -> (Edge output -> Graph (Vertex input)) -> Graph (Vertex input)
using outputVertex@(Vertex outputVertexID) continuation = do
  outputVertexDef <- getVertexDef outputVertex

  thisVertex@(Vertex thisVertexID) <- continuation $ Direct (vertexDefInputQueue outputVertexDef)

  updateVertexDef outputVertex (\VertexDef{..} -> VertexDef{vertexDefInputs = thisVertexID : vertexDefInputs, ..})
  updateVertexDef thisVertex (\VertexDef{..} -> VertexDef{vertexDefOutputs = outputVertexID : vertexDefOutputs, .. })
  return thisVertex

inputVertex :: Graph (Vertex i) -> Graph (Input i)
inputVertex vref = do
  vtx <- getVertexDef =<< vref
  return (Input $ vertexDefInputQueue vtx)

{-# INLINE send #-}
send :: Show i => Edge i -> Timestamp -> i -> Node ()
send (Direct inputQueue) timestamp i = Node . lift $ writeTQueue inputQueue (timestamp, i)
send (Mapped f inputQueue) timestamp i = Node . lift $ writeTQueue inputQueue (timestamp, f i)

output :: (Eq o, Show o) => ([o] -> STM ()) -> Graph (Vertex o)
output stmAction =
  vertex
    (Data.Map.Strict.empty :: Data.Map.Strict.Map Timestamp [o])
    (\timestamp o state ->
      return $ Data.Map.Strict.alter (\case
                                        Nothing -> Just [o]
                                        Just accum -> Just (o : accum)
                                      ) timestamp state
    ) (\timestamp state -> do
        Node . lift $ stmAction $ state Data.Map.Strict.! timestamp
        return $ Data.Map.Strict.delete timestamp state
    )
