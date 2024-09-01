{-# LANGUAGE ScopedTypeVariables #-}

module Dataflow.OperatorsSpec (spec) where

import           Dataflow.Operators    (fanout, fold)
import           Prelude               hiding (map)

import           Test.Dataflow         (runDataflow)
import           Test.Hspec
import           Test.QuickCheck       hiding (discard)


spec :: Spec
spec = do
  describe "fanout" $
    it "sends all input to each output" $ property $ do
      let fanout' next = fanout [next, next, next]

      \(numbers :: [Int]) -> do
        actual <- runDataflow fanout' numbers
        actual `shouldMatchList` (numbers <> numbers <> numbers)

  describe "fold" $
    it "collects data and emits it at the end" $ property $ do
      \(numbers :: [Int]) -> do
        let summate = fold 0 (+) id

        runDataflow summate numbers `shouldReturn` [sum numbers]
