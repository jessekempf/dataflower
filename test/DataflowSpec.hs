{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecursiveDo         #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataflowSpec (spec) where

import           Control.Concurrent.STM.TVar (newTVarIO, readTVarIO)
import           Control.Monad               (void, (>=>))
import           Dataflow
import           Prelude
import           Control.Concurrent.STM      (modifyTVar')
import qualified Data.Map.Strict
import           Test.Dataflow               (runDataflow)
import           Test.Hspec
import           Test.QuickCheck             hiding (discard)

spec :: Spec
spec = do
  it "can pass through data at all" $ do
    runDataflow passthrough ["hello world"] `shouldReturn` ["hello world"]

  it "can pass through data without modification" $ property $ do
    \(numbers :: [Integer]) -> do
      results <- runDataflow passthrough numbers
      results `shouldMatchList` numbers

  describe "execute" $ do
    it "isolates the state of runs from each other" $ property $ \(NonEmpty numbers) -> do
      out     <- newTVarIO []
      program <- compile (outputSTM (\(results :: [Int])-> modifyTVar' out (results :)))

      void $ synchronize =<< execute numbers =<< execute numbers program

      [result1, result2] <- readTVarIO out

      result1 `shouldMatchList` numbers
      result2 `shouldMatchList` numbers

    it "bundles all the execution state into a Program" $ property $ \(NonEmpty numbers) -> do
      out     <- newTVarIO 0
      program <- compile (integrate =<< outputSTM (\results -> modifyTVar' out (+ sum results)))

      void $ execute numbers program >>= execute numbers >>= execute numbers >>= synchronize

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

passthrough :: (Eq i, Show i) => VertexReference i -> Dataflow (VertexReference i)
passthrough nextVertex = mdo
  inputVertex <- vertex () (\ts i _ -> send next ts i) (\_ _ -> return ())
  next <- connect inputVertex nextVertex
  return inputVertex

storeAndForward :: (Eq i, Show i) => VertexReference i -> Dataflow (VertexReference i)
storeAndForward nextVertex = mdo
  inputVertex <- vertex Data.Map.Strict.empty (\t i s ->
    return $ Data.Map.Strict.alter (\case
                                                    Nothing -> Just [i]
                                                    Just accum -> Just (i : accum)
                                                  ) t s
    ) (\t s -> do
      mapM_ (send next t) (Data.Map.Strict.findWithDefault [] t s)
      return $ Data.Map.Strict.delete t s
    )
  next <- connect inputVertex nextVertex
  return inputVertex

integrate :: VertexReference Int -> Dataflow (VertexReference Int)
integrate nextVertex = mdo
  inputVertex <- vertex Data.Map.Strict.empty (\timestamp i accumulators ->
      return $ Data.Map.Strict.alter (\case
                                        Nothing -> Just i
                                        Just accum -> Just (accum + i)
                                      ) timestamp accumulators
    ) (\timestamp accumulators -> do
        send next timestamp (accumulators Data.Map.Strict.! timestamp)
        return $ Data.Map.Strict.delete timestamp accumulators
    )
  next <- connect inputVertex nextVertex
  return inputVertex

discard :: (Eq i, Show i) => VertexReference () -> Dataflow (VertexReference i)
discard _ = vertex () (\_ _ _ -> return ()) (const $ const $ return ())
