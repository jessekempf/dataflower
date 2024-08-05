{-# LANGUAGE NumericUnderscores #-}

module Main where

import           Control.Monad      (void)
import           Data.Monoid        (Sum (..))
import           Dataflow.Operators (mcollect)
import           Prelude
import           Test.Dataflow      (runDataflowMany)


main :: IO ()
main = do
  print =<< runDataflowMany mcollect (replicate 100 $ map Sum [0..1_000_000   :: Int])
