
{-# LANGUAGE ImpredicativeTypes  #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
  statelessVertex,
  statefulVertex,
) where

import           Control.Monad       (forM_)
import           Data.Map.Strict     (Map, delete, empty, findWithDefault,
                                      insert)
import           Dataflow.Primitives (Edge, Graph, Node, Timestamp, Vertex,
                                      send, using, vertex)

-- | Construct a stateless vertex that sends each input to every 'Edge' in the output list.
--
-- @since 0.1.3.0
fanout :: forall a. (Eq a, Show a) => [Vertex a] -> Graph (Vertex a)
fanout nextVertices = go nextVertices []
  where
    go :: [Vertex a] -> [Edge a] -> Graph (Vertex a)
    go [] edges = statelessVertex (\timestamp a -> forM_ edges (\edge -> send edge timestamp a))
    go (vtx : vtxs) edges = using vtx (\edge -> go vtxs (edge : edges))

fold :: (Eq i, Show i, Eq o, Show o, Show state) => state -> (i -> state -> state) -> (state -> o) -> Vertex o -> Graph (Vertex i)
fold zeroState accumulate output nextVertex =
  using nextVertex $ \next ->
    statefulVertex zeroState
      (\_ i state -> return (accumulate i state))
      (\timestamp state -> send next timestamp (output state))

mcollect :: (Eq a, Show a, Monoid a) => Vertex a -> Graph (Vertex a)
mcollect = fold mempty mappend id

statelessVertex :: (Eq i, Show i) => (Timestamp -> i -> Node()) -> Graph (Vertex i)
statelessVertex onRecv = vertex () (\t i _ -> onRecv t i) (\_ _ -> return ())

statefulVertex :: (Eq i, Show i) => state -> (Timestamp -> i -> state -> Node state) -> (Timestamp -> state -> Node ()) -> Graph (Vertex i)
statefulVertex zeroState onRecv onNotify =
  vertex (empty :: Map Timestamp state)
    (\t i stateMap -> do
      state' <- onRecv t i (Data.Map.Strict.findWithDefault zeroState t stateMap)
      return $ Data.Map.Strict.insert t state' stateMap
    ) (\t stateMap -> do
      onNotify t (Data.Map.Strict.findWithDefault zeroState t stateMap)
      return $ Data.Map.Strict.delete t stateMap
    )
