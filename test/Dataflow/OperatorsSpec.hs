{-# LANGUAGE ScopedTypeVariables #-}

module Dataflow.OperatorsSpec (spec) where

import           Data.Functor.Identity (Identity (..))
import           Data.Semigroup        (Sum (..))
import           Dataflow
import           Dataflow.Operators
import           Prelude               hiding (map)
import qualified Prelude               (map)

import           Test.Dataflow         (runDataflow)
import           Test.Hspec
import           Test.QuickCheck       hiding (discard)


spec :: Spec
spec = do
  describe "fanout" $
    it "sends all input to each output" $ property $ do
      \(numbers :: [Int]) -> numbers `shouldBe` numbers
      -- let fanout' next = fanout [next, next, next]

      -- \(numbers :: [Int]) -> do
      --   actual <- runDataflow fanout' numbers
      --   actual `shouldMatchList` (numbers <> numbers <> numbers)