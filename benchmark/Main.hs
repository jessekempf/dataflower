{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE DataKinds #-}
module Main where

import           Criterion.Main
import           Dataflow
import           Prelude
import           Test.Dataflow  (runDataflow)
import Control.Monad.IO.Class (MonadIO)
import Dataflow (Phase(Running))
import Control.Monad (void)


main :: IO ()
main = do
  defaultMain [
    envWithCleanup setupEnv teardownEnv $ \ ~(passthrough, blackhole) -> 
      bgroup "dataflow" [
        bench "passthrough     1,000 inputs" $ nfIO (runDataflow passthrough [0..1000    :: Int]),
        bench "discard         1,000 inputs" $ nfIO (runDataflow blackhole   [0..1000    :: Int]),
        bench "passthrough 1,000,000 inputs" $ nfIO (runDataflow passthrough [0..1000000 :: Int]),
        bench "discard     1,000,000 inputs" $ nfIO (runDataflow blackhole   [0..1000000 :: Int])
      ]
    ]
  where
    setupEnv = do
      passthroughGraph <- start =<< prepare (inputVertex . passthrough =<< discard)
      blackholeGraph <- start =<< prepare (inputVertex . blackhole =<< discard)
      return (passthroughGraph, blackholeGraph)

    teardownEnv (passthroughGraph, blackholeGraph) = do
      stop passthroughGraph
      stop blackholeGraph

    passthrough :: (Eq a, Show a) => Vertex a -> Graph (Vertex a)
    passthrough nextVertex =
      using nextVertex $ \next ->
        vertex () (\timestamp i _ -> send next timestamp i) (const $ const $ return ())

    blackhole :: (Eq a, Show a) => Vertex a -> Graph (Vertex a)
    blackhole _ = discard

    discard :: Graph (Vertex a)
    discard = vertex () (\_ _ _ -> return ()) (const $ const $ return ())

    runDataflow :: (Eq i, Show i, MonadIO io) => Program 'Running i -> [i] -> io ()
    runDataflow dataflow inputs = void $ synchronize =<< submit inputs dataflow