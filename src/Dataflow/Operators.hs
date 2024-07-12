
{-# LANGUAGE ImpredicativeTypes #-}
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
) where

import           Dataflow.Primitives (Dataflow, Edge, send, vertex)
import Control.Monad (forM_)

-- | Construct a stateless vertex that sends each input to every 'Edge' in the output list.
--
-- @since 0.1.3.0
fanout :: Show i => [Edge i] -> Dataflow (Edge i)
fanout nexts = vertex ()
  (\timestamp a _ -> forM_ nexts (\next -> send next timestamp a))
  (\_ _ -> return ())