
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
import           Data.Map.Strict     (Map, alter, delete, empty, lookup)
import           Data.Maybe          (fromMaybe)
import           Dataflow.Primitives (Dataflow, Timestamp, VertexReference,
                                      connect, send, vertex)

-- | Construct a stateless vertex that sends each input to every 'Edge' in the output list.
--
-- @since 0.1.3.0
fanout :: (Eq i, Show i) => [VertexReference i] -> Dataflow (VertexReference i)
fanout nextVertices = mdo
  inputVertex <- vertex () (\timestamp a _ -> forM_ nexts (\next -> send next timestamp a)) (\_ _ -> return ())
  nexts <- mapM (connect inputVertex) nextVertices
  return inputVertex

fold :: (Eq i, Show i, Eq o, Show o, Show state) => state -> (i -> state -> state) -> (state -> o) -> VertexReference o -> Dataflow (VertexReference i)
fold zeroState accumulate output nextVertex = mdo
  inputVertex <- vertex (empty :: Map Timestamp state)
                    (\timestamp i stateMap -> do
                      let stateMap' = Data.Map.Strict.alter (\case
                                                        Nothing -> Just (accumulate i zeroState)
                                                        Just state -> Just (accumulate i state)
                                                    ) timestamp stateMap
                      return stateMap'
                    ) (\timestamp stateMap -> do
                          send next timestamp (output $ fromMaybe zeroState (Data.Map.Strict.lookup timestamp stateMap))
                          return $ Data.Map.Strict.delete timestamp stateMap
                      )
  next <- connect inputVertex nextVertex
  return inputVertex

mcollect :: (Eq a, Show a, Monoid a) => VertexReference a -> Dataflow (VertexReference a)
mcollect = fold mempty mappend id
