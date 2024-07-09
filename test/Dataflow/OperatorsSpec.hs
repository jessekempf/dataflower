{-# LANGUAGE ScopedTypeVariables #-}

module Dataflow.OperatorsSpec (spec) where

import           Data.Functor.Identity (Identity (..))
import           Data.Semigroup        (Sum (..))
import           Dataflow
import           Dataflow.Operators
import           Prelude               hiding (map)
import qualified Prelude               (map)

import           Test.Dataflow         (runDataflow, bypass2, bypass3)
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

  describe "map" $
    it "applies a function to all values passing through" $ property $
      \(numbers :: [Int]) -> runDataflow (map (+ 1)) numbers `shouldReturn` Prelude.map (+ 1) numbers

  describe "join2" $
    it "accepts two different kinds of input" $ property $ \numbers -> do
      let numJoin next = join2 (0 :: Int)
                          (\sref _ i            -> modifyState sref (+ i))
                          (\sref _ (Identity j) -> modifyState sref (+ j))
                          (\sref t -> do
                            send next t =<< readState sref
                            finalize next t
                          )
          numbers'     = Prelude.map Identity numbers

      runDataflow (\next -> do
        (i, j) <- numJoin next
        bypass2 i j  
        ) numbers  `shouldReturn` [sum numbers]
      runDataflow (\next -> do
        (i, j) <- numJoin next
        bypass2 j i
        ) numbers' `shouldReturn` [sum numbers]

  describe "join3" $
    it "accepts three different kinds of input" $ property $ \numbers -> do
      let numJoin next   = join3 (0 :: Int)
                            (\sref _ i            -> modifyState sref (+ i))
                            (\sref _ (Identity j) -> modifyState sref (+ j))
                            (\sref _ (Sum k)      -> modifyState sref (+ k))
                            (\sref t -> do
                              send next t =<< readState sref
                              finalize next t
                            )
          numbers'       = Prelude.map Identity numbers
          numbers''      = Prelude.map Sum numbers

      runDataflow (\next -> do
        (i, j, k) <- numJoin next
        bypass3 i j k
        ) numbers   `shouldReturn` [sum numbers]
      runDataflow (\next -> do
        (i, j, k) <- numJoin next
        bypass3 j k i
        ) numbers'  `shouldReturn` [sum numbers]
      runDataflow (\next -> do
        (i, j, k) <- numJoin next
        bypass3 k i j
        ) numbers'' `shouldReturn` [sum numbers]
