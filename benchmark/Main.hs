{-# LANGUAGE RecursiveDo #-}
module Main where

import           Criterion.Main
import           Dataflow
import           Prelude
import           Test.Dataflow  (runDataflow)


main :: IO ()
main = defaultMain [
  bgroup "dataflow" [
      bench "passthrough     1,000 inputs" $ nfIO (runDataflow passthrough [0..1000    :: Int]),
      bench "discard         1,000 inputs" $ nfIO (runDataflow blackhole   [0..1000    :: Int]),
      bench "passthrough 1,000,000 inputs" $ nfIO (runDataflow passthrough [0..1000000 :: Int]),
      bench "discard     1,000,000 inputs" $ nfIO (runDataflow blackhole   [0..1000000 :: Int])
    ]
  ]
  where
    passthrough :: (Eq a, Show a) => VertexReference a -> Dataflow (VertexReference a)
    passthrough nextVertex = mdo
      inputVertex <- vertex () (\timestamp i _ -> send next timestamp i) (const $ const $ return ())
      next <- connect inputVertex nextVertex
      return inputVertex

    blackhole :: (Eq a, Show a) => VertexReference a -> Dataflow (VertexReference a)
    blackhole _ = mdo
      inputVertex <- vertex () (\_ _ _ -> return ()) (const $ const $ return ())
      return inputVertex
