{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecursiveDo #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE BangPatterns #-}

module DataflowSpec (spec) where

import           Control.Concurrent.STM.TVar (newTVarIO, readTVarIO)
import           Control.Monad               (void, (>=>))
import           Dataflow
import           Prelude

import           Test.Dataflow               (runDataflow)
import           Test.Hspec
import           Test.QuickCheck             hiding (discard)
import           Test.QuickCheck.Modifiers   (NonEmptyList (..))
import Debug.Trace (traceM, traceShowM)
import Text.Printf (printf)
import qualified Data.Map.Strict
import Data.List (sort, permutations)
import Control.Concurrent.STM (modifyTVar')
import Control.Concurrent (threadDelay)

spec :: Spec
spec = do
  -- it "can pass through data at all" $ do
  --   runDataflow passthrough ["hello world"] `shouldReturn` ["hello world"]

  -- it "can pass through data without modification" $ property $ do
  --   \(numbers :: [Integer]) -> do
  --     traceM ""
  --     traceM (printf "testing: %s" (show numbers))
  --     results <- runDataflow passthrough numbers
  --     traceM (printf "tested: %s, got %s [%s]" (show numbers) (show results) (show $ sort numbers == sort results))

  --     results `shouldMatchList` numbers

  describe "execute" $ do
    it "isolates the state of runs from each other" $ property $ \(NonEmpty numbers) -> do
      out     <- newTVarIO []
      program <- compile (outputSTM (\(results :: [Int]) -> modifyTVar' out (results:)))

      void $ synchronize =<< execute numbers =<< execute numbers program

      -- traceShowM =<< readTVarIO out
      -- threadDelay 2

      [result1, result2] <- readTVarIO out

      result1 `shouldMatchList` numbers
      result2 `shouldMatchList` numbers

--     it "bundles all the execution state into a Program" $ property $ \(NonEmpty numbers) -> do
--       out     <- newTVarIO 0
--       program <- compile (integrate =<< outputTVar const out)

--       void $ execute numbers program >>= execute numbers >>= execute numbers

--       readTVarIO out `shouldReturn` (3 * sum numbers)

--   describe "finalize" $ do
--     it "finalizes vertices" $ property $
--       \(numbers :: [Int]) -> runDataflow storeAndForward numbers `shouldReturn` numbers

--     it "finalizes vertices in the correct order" $ property $
--       \(numbers :: [Int]) ->
--         runDataflow (storeAndForward >=> storeAndForward >=> storeAndForward) numbers `shouldReturn` numbers

--   describe "discard" $
--     it "discards all input" $ property $
--       \(numbers :: [Int]) -> runDataflow discard numbers `shouldReturn` ([] :: [()])

passthrough :: (Eq i, Show i) => VertexReference i -> Dataflow (VertexReference i)
passthrough nextVertex = mdo
  traceM "configuring passthrough"
  inputVertex <- vertex () (\ts i _ -> send next ts i) (\_ _ -> return ())
  next <- connect inputVertex nextVertex
  traceM "recursive connected"
  return inputVertex

-- storeAndForward :: Edge i -> Dataflow (Edge i)
-- storeAndForward next = statefulVertex [] store forward
--   where
--     store sref _ i = modifyState sref (i :)
--     forward sref t = do
--       mapM_ (send next t) . reverse =<< readState sref
--       writeState sref []
--       finalize next t

integrate :: VertexReference Int -> Dataflow (VertexReference Int)
integrate nextVertex = mdo
  inputVertex <- vertex Data.Map.Strict.empty (\timestamp i accumulators -> do
      let accumulators' = Data.Map.Strict.alter (\case
                                                  Nothing -> Just i
                                                  Just accum -> Just (accum + i)
                                                ) timestamp accumulators
      send next timestamp (accumulators' Data.Map.Strict.! timestamp)
      return accumulators'
    ) (\timestamp accumulators -> return $ Data.Map.Strict.delete timestamp accumulators)
  next <- connect inputVertex nextVertex
  return inputVertex

-- discard :: Edge () -> Dataflow (Edge i)
-- discard next = statelessVertex (\_ _ -> return ()) (finalize next)

