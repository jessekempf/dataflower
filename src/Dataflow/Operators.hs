
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE RecursiveDo        #-}
{-|
Module      : Dataflow
Description : Timely Dataflow for Haskell
Copyright   : (c) Double Crown Gaming Co. 2020
License     : BSD3
Maintainer  : jesse.kempf@doublecrown.co
Stability   : experimental

Common utility operators for data flows

@since 0.1.3.0
-}

module Dataflow.Operators (
  fanout,
  fold,
  mcollect,
) where

import           Control.Monad       (forM_)
import           Data.Map.Strict     (Map, delete, empty, findWithDefault, insert)
import           Dataflow.Primitives (Dataflow, Timestamp, VertexReference, Node,
                                      connect, send, vertex)

-- | Construct a stateless vertex that sends each input to every 'Edge' in the output list.
--
-- @since 0.1.3.0
fanout :: (Eq i, Show i) => [VertexReference i] -> Dataflow (VertexReference i)
fanout nextVertices = mdo
  inputVertex <- statelessVertex (\timestamp a -> forM_ nexts (\next -> send next timestamp a))
  nexts <- mapM (connect inputVertex) nextVertices
  return inputVertex

fold :: (Eq i, Show i, Eq o, Show o, Show state) => state -> (i -> state -> state) -> (state -> o) -> VertexReference o -> Dataflow (VertexReference i)
fold zeroState accumulate output nextVertex = mdo
  inputVertex <- statefulVertex zeroState
    (\_ i state -> return (accumulate i state))
    (\timestamp state -> send next timestamp (output state))
  next <- connect inputVertex nextVertex
  return inputVertex

mcollect :: (Eq a, Show a, Monoid a) => VertexReference a -> Dataflow (VertexReference a)
mcollect = fold mempty mappend id

statelessVertex :: (Eq i, Show i) => (Timestamp -> i -> Node()) -> Dataflow (VertexReference i)
statelessVertex onRecv = vertex () (\t i _ -> onRecv t i) (\_ _ -> return ())

statefulVertex :: (Eq i, Show i) => state -> (Timestamp -> i -> state -> Node state) -> (Timestamp -> state -> Node ()) -> Dataflow (VertexReference i)
statefulVertex zeroState onRecv onNotify =
  vertex (empty :: Map Timestamp state)
    (\t i stateMap -> do
      state' <- onRecv t i (Data.Map.Strict.findWithDefault zeroState t stateMap)
      return $ Data.Map.Strict.insert t state' stateMap
    ) (\t stateMap -> do
      onNotify t (Data.Map.Strict.findWithDefault zeroState t stateMap)
      return $ Data.Map.Strict.delete t stateMap
    )