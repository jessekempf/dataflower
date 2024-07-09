{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE InstanceSigs #-}

module Dataflow.Primitives (
  Dataflow(..),
  DataflowState,
  Vertex(..),
  initDataflowState,
  duplicateDataflowState,
  StateRef,
  newState,
  readState,
  writeState,
  modifyState,
  Edge,
  Egress,
  Feedback,
  Epoch(..),
  Timestamp(..),
  inc,
  registerVertex,
  input,
  send,
  finalize,
  loop,
  egress,
  finalizeEgress,
  feedback,
  finalizeFeedback,
) where

import           Control.Arrow              ((>>>))
import           Control.Monad              (forM, (>=>))
import           Control.Monad.IO.Class     (liftIO)
import Control.Monad.Fix (MonadFix)
import           Control.Monad.State.Strict (StateT, get, gets, modify, MonadFix (..))
import           Control.Monad.Trans        (lift)
import           Data.Hashable              (Hashable (..))
import           Data.IORef                 (IORef, modifyIORef', newIORef,
                                             readIORef, writeIORef)
import           Data.Vector                (Vector, empty, snoc, unsafeIndex)
import           GHC.Exts                   (Any)
import           Numeric.Natural            (Natural)
import           Prelude
import           Unsafe.Coerce              (unsafeCoerce)
import Data.Vector.Generic.New (run)


newtype VertexID    = VertexID        Int deriving (Eq, Ord, Show)
newtype StateID     = StateID         Int deriving (Eq, Ord, Show)
newtype Epoch       = Epoch       Natural deriving (Eq, Ord, Hashable, Show)
newtype LoopCounter = LoopCounter Natural deriving (Eq, Ord, Hashable, Show)
-- | 'Timestamp's represent instants in the causal timeline.
--
-- @since 0.1.0.0
data Timestamp   = Timestamp     Epoch [LoopCounter] deriving (Eq, Ord, Show)

instance Hashable Timestamp where
  hashWithSalt salt (Timestamp epoch counters) = hashWithSalt salt epoch ^ hashWithSalt salt counters


-- | An 'Edge' is a typed reference to a computational vertex that
-- takes 'a's as its input.
--
-- @since 0.1.0.0
newtype Edge a      = Edge       VertexID

-- | Class of entities that can be incremented by one.
class Incrementable a where
  inc :: a -> a

instance Incrementable VertexID where
  inc (VertexID n) = VertexID (n + 1)

instance Incrementable StateID where
  inc (StateID n) = StateID (n + 1)

instance Incrementable Epoch where
  inc (Epoch n) = Epoch (n + 1)

instance Incrementable LoopCounter where
  inc (LoopCounter n) = LoopCounter (n + 1)

data DataflowState = DataflowState {
  dfsVertices       :: Vector Any,
  dfsStates         :: Vector (IORef Any),
  dfsLastVertexID   :: VertexID,
  dfsLastStateID    :: StateID
}

-- | `Dataflow` is the type of all dataflow operations.
--
-- @since 0.1.0.0
newtype Dataflow a = Dataflow { runDataflow :: StateT DataflowState IO a }
  deriving (Functor, Applicative, Monad)

initDataflowState :: DataflowState
initDataflowState = DataflowState {
  dfsVertices       = empty,
  dfsStates         = empty,
  dfsLastVertexID   = VertexID (-1),
  dfsLastStateID    = StateID (-1)
}

duplicateDataflowState :: Dataflow DataflowState
duplicateDataflowState = Dataflow $ do
  DataflowState{..} <- get

  newStates <- liftIO $ forM dfsStates dupIORef

  return $ DataflowState { dfsStates = newStates, .. }

  where
    dupIORef = readIORef >=> newIORef

data Vertex i =
  forall s. StatefulVertex
      (StateRef s)
      (StateRef s -> Timestamp -> i -> Dataflow ())
      (StateRef s -> Timestamp -> Dataflow())
  | LoopContext    (Nested (Edge i))
  | StatelessVertex
      (Timestamp -> i -> Dataflow ())
      (Timestamp -> Dataflow ())

-- | Retrieve the vertex for a given edge.
lookupVertex :: Edge i -> Dataflow (Vertex i)
lookupVertex (Edge (VertexID vindex)) =
  Dataflow $ do
    vertices <- gets dfsVertices

    return $ unsafeCoerce (vertices `unsafeIndex` vindex)

-- | Store a provided vertex and obtain an 'Edge' that refers to it.
registerVertex :: Vertex i -> Dataflow (Edge i)
registerVertex vertex =
  Dataflow $ do
    vid <- gets (dfsLastVertexID >>> inc)

    modify $ addVertex vertex vid

    return (Edge vid)

  where
    addVertex vtx vid s = s {
      dfsVertices     = dfsVertices s `snoc` unsafeCoerce vtx,
      dfsLastVertexID = vid
    }

-- | Mutable state that holds an `a`.
--
-- @since 0.1.0.0
newtype StateRef a = StateRef StateID

-- | Create a `StateRef` initialized to the provided `a`.
--
-- @since 0.1.0.0
newState :: a -> Dataflow (StateRef a)
newState a =
  Dataflow $ do
    sid   <- gets (dfsLastStateID >>> inc)
    ioref <- lift $ newIORef (unsafeCoerce a)

    modify $ addState ioref sid

    return (StateRef sid)

  where
    addState ref sid s = s {
      dfsStates      = dfsStates s `snoc` ref,
      dfsLastStateID = sid
    }

lookupStateRef :: StateRef s -> Dataflow (IORef Any)
lookupStateRef (StateRef (StateID sindex)) =
  Dataflow $ do
    states <- gets dfsStates

    return (states `unsafeIndex` sindex)

-- | Read the value stored in the `StateRef`.
--
-- @since 0.1.0.0
readState :: StateRef a -> Dataflow a
readState sref = do
  ioref <- lookupStateRef sref
  Dataflow $ lift (unsafeCoerce <$> readIORef ioref)

-- | Overwrite the value stored in the `StateRef`.
--
-- @since 0.1.0.0
writeState :: StateRef a -> a -> Dataflow ()
writeState sref x = do
  ioref <- lookupStateRef sref
  Dataflow $ lift $ writeIORef ioref (unsafeCoerce x)

-- | Update the value stored in `StateRef`.
--
-- @since 0.1.0.0
modifyState :: StateRef a -> (a -> a) -> Dataflow ()
modifyState sref op = do
  ioref <- lookupStateRef sref
  Dataflow $ lift $ modifyIORef' ioref (unsafeCoerce . op . unsafeCoerce)

{-# INLINEABLE input #-}
input :: Traversable t => Timestamp -> t i -> Edge i -> Dataflow ()
input timestamp inputs next = do
  mapM_ (send next timestamp) inputs
  finalize next timestamp

{-# INLINE send #-}
-- | Send an `input` item to be worked on to the indicated vertex.
--
-- @since 0.1.0.0
send :: Edge input -> Timestamp -> input -> Dataflow ()
send e t i = lookupVertex e >>= invoke t i
  where
    invoke timestamp datum (StatefulVertex sref callback _) = callback sref timestamp datum
    invoke timestamp datum (LoopContext defineLoop) = do
      loopInput <- runNested defineLoop
      send loopInput (nestLoop timestamp) datum
    invoke timestamp datum (StatelessVertex callback _)     = callback timestamp datum

    nestLoop (Timestamp epoch lcs) = Timestamp epoch (LoopCounter 0 : lcs)
-- Notify all relevant vertices that no more input is coming for `Timestamp`.
--
-- @since 0.1.0.0
finalize :: Edge i -> Timestamp -> Dataflow ()
finalize e t = lookupVertex e >>= invoke t
  where
    invoke timestamp (StatefulVertex sref _ finalizer) = finalizer sref timestamp
    invoke timestamp (LoopContext defineLoop) = do
      loopInput <- runNested defineLoop
      finalize loopInput timestamp
    invoke timestamp (StatelessVertex _ finalizer) = finalizer timestamp

newtype Feedback a = Feedback (Edge a)
newtype Egress a = Egress (Edge a)
newtype Nested a = Nested { runNested ::  Dataflow a } deriving (Functor, Applicative, Monad)

instance MonadFix Nested where
  mfix :: (a -> Nested a) -> Nested a
  mfix execute = Nested $ Dataflow $ mfix (runDataflow . runNested . execute)


{-# INLINE feedback #-}
{-# ANN feedback "HLint: ignore Eta reduce" #-}
feedback :: Feedback feedback -> Timestamp -> feedback -> Dataflow ()
feedback (Feedback f) t datum = send f (incLoopCounter t) datum
  where
    incLoopCounter (Timestamp _ []) = error "programming error: incrementing non-existent loop counter"
    incLoopCounter (Timestamp e (counter : lcs)) = Timestamp e (inc counter : lcs)

finalizeFeedback :: Feedback feedback -> Timestamp -> Dataflow ()
finalizeFeedback (Feedback f) = finalize f

{-# INLINE egress #-}
{-# ANN egress "HLint: ignore Eta reduce" #-}
egress :: Egress o -> Timestamp -> o -> Dataflow ()
egress (Egress o) t datum = send o (unnestLoop t) datum
  where
    unnestLoop (Timestamp _ []) = error "programming error: unnesting non-nested timestamp"
    unnestLoop (Timestamp e (_ : lcs)) = Timestamp e lcs

finalizeEgress :: Egress o -> Timestamp -> Dataflow ()
finalizeEgress (Egress o) = finalize o

loop :: (Egress o -> Feedback i -> Dataflow (Edge i)) -> Edge o -> Dataflow (Edge i)
loop nestedFlow out =
  registerVertex $ LoopContext (mdo
    input <- Nested $ nestedFlow (Egress out) (Feedback input)
    return input
  )
