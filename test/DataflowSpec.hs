{-# LANGUAGE ScopedTypeVariables #-}

module DataflowSpec (spec) where

import           Control.Concurrent.STM.TVar (newTVarIO, readTVarIO)
import           Control.Monad               (void, (>=>))
import           Dataflow
import           Prelude

import           Test.Dataflow               (runDataflow)
import           Test.Hspec
import           Test.QuickCheck             hiding (discard)
import           Test.QuickCheck.Modifiers   (NonEmptyList (..))
import Debug.Trace (traceM)

spec :: Spec
spec = do
  it "can pass through data without modification" $ property $ do
    let passthrough next = statelessVertex (send next) (finalize next)

    \(numbers :: [Integer]) -> runDataflow passthrough numbers `shouldReturn` numbers

  describe "execute" $ do
    it "isolates the state of runs from each other" $ property $ \(NonEmpty numbers) -> do
      out     <- newTVarIO []
      program <- compile (integrate =<< outputTVar (:) out)

      void $ execute numbers program
      void $ execute numbers program

      (reverse <$> readTVarIO out) `shouldReturn` (scanl1 (+) numbers ++ scanl1 (+) numbers)

    it "bundles all the execution state into a Program" $ property $ \(NonEmpty numbers) -> do
      out     <- newTVarIO 0
      program <- compile (integrate =<< outputTVar const out)

      void $ execute numbers program >>= execute numbers >>= execute numbers

      readTVarIO out `shouldReturn` (3 * sum numbers)

  describe "finalize" $ do
    it "finalizes vertices" $ property $
      \(numbers :: [Int]) -> runDataflow storeAndForward numbers `shouldReturn` numbers

    it "finalizes vertices in the correct order" $ property $
      \(numbers :: [Int]) ->
        runDataflow (storeAndForward >=> storeAndForward >=> storeAndForward) numbers `shouldReturn` numbers

  describe "discard" $
    it "discards all input" $ property $
      \(numbers :: [Int]) -> runDataflow discard numbers `shouldReturn` ([] :: [()])

  describe "loop" $
    it "loops" $ do
      runDataflow (loop (feedbackCeiling 5)) [1, 2, 3, 4, 5, 6]
        `shouldReturn` [5, 5, 5, 5, 5, 10]

feedbackCeiling :: Int -> Egress Int -> Feedback Int -> Dataflow (Edge Int)
feedbackCeiling ceil e f =
  statelessVertex (\t (i :: Int) ->
    if i `mod` ceil == 0 then egress e t i else feedback f t (i + 1)
  ) (finalizeFeedback f >> finalizeEgress e)

storeAndForward :: Edge i -> Dataflow (Edge i)
storeAndForward next = statefulVertex [] store forward
  where
    store sref _ i = modifyState sref (i :)
    forward sref t = do
      mapM_ (send next t) . reverse =<< readState sref
      writeState sref []
      finalize next t

integrate :: Edge Int -> Dataflow (Edge Int)
integrate next = statefulVertex 0 recv (const $ finalize next)
  where
    recv s t i   = do
      modifyState s (+ i)

      send next t =<< readState s

discard :: Edge () -> Dataflow (Edge i)
discard next = statelessVertex (\_ _ -> return ()) (finalize next)

