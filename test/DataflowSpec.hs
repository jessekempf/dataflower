{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module DataflowSpec (spec) where

import           Control.Concurrent.STM      (modifyTVar')
import           Control.Concurrent.STM.TVar (newTVarIO, readTVarIO)
import           Control.Monad               (void, (>=>))
import qualified Data.Map.Strict
import           Dataflow
import           Dataflow.Operators          (mcollect)
import           Prelude
import           Test.Dataflow               (runDataflow, runDataflowMany)
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

  describe "submit" $ do
    it "isolates the state of runs from each other" $ property $ \(NonEmpty numbers) -> do
      out     <- newTVarIO []
      program <- start =<< prepare (inputVertex $ Dataflow.output (\(results :: [Int])-> modifyTVar' out (results :)))

      void $ synchronize =<< submit numbers =<< submit numbers program

      [result1, result2] <- readTVarIO out

      result1 `shouldMatchList` numbers
      result2 `shouldMatchList` numbers

    it "bundles all the execution state into a Program" $ property $ \(NonEmpty numbers) -> do
      out     <- newTVarIO 0
      program <- start =<< prepare ((inputVertex . integrate) =<< Dataflow.output (\results -> modifyTVar' out (+ sum results)))

      void $ submit numbers program >>= submit numbers >>= submit numbers >>= stop

      readTVarIO out `shouldReturn` (3 * sum numbers)

  describe "runDataflowMany" $ do
    it "leads to the production of only as many outputs as inputs" $ property $ \(NonEmpty (numbers :: [Sum Int])) -> do
      runDataflowMany mcollect (replicate 1 numbers) `shouldReturn` [[sum numbers]]
      runDataflowMany mcollect (replicate 2 numbers) `shouldReturn` [[sum numbers], [sum numbers]]

  describe "finalize" $ do
    it "finalizes vertices" $ property $
      \(numbers :: [Int]) -> runDataflow storeAndForward numbers `shouldReturn` numbers

    it "finalizes vertices in the correct order" $ property $
      \(numbers :: [Int]) ->
        runDataflow (storeAndForward >=> storeAndForward >=> storeAndForward) numbers `shouldReturn` numbers

  describe "discard" $
    it "discards all input" $ property $
      \(numbers :: [Int]) -> runDataflow discard numbers `shouldReturn` ([] :: [()])

passthrough :: (Eq i, Show i) => Vertex i -> Graph (Vertex i)
passthrough nextVertex =
  using nextVertex $ \next -> 
    vertex () (\ts i _ -> send next ts i) (\_ _ -> return ())

storeAndForward :: (Eq i, Show i) => Vertex i -> Graph (Vertex i)
storeAndForward nextVertex =
  using nextVertex $ \next ->
    vertex Data.Map.Strict.empty (\t i s ->
      return $ Data.Map.Strict.alter (\case
                                                      Nothing -> Just [i]
                                                      Just accum -> Just (i : accum)
                                                    ) t s
      ) (\t s -> do
        mapM_ (send next t) (Data.Map.Strict.findWithDefault [] t s)
        return $ Data.Map.Strict.delete t s
      )

integrate :: Vertex Int -> Graph (Vertex Int)
integrate nextVertex =
  using nextVertex $ \next ->
    vertex Data.Map.Strict.empty (\timestamp i accumulators ->
      return $ Data.Map.Strict.alter (\case
                                        Nothing -> Just i
                                        Just accum -> Just (accum + i)
                                      ) timestamp accumulators
    ) (\timestamp accumulators -> do
        send next timestamp (accumulators Data.Map.Strict.! timestamp)
        return $ Data.Map.Strict.delete timestamp accumulators
    )

discard :: (Eq i, Show i) => Vertex () -> Graph (Vertex i)
discard _ = vertex () (\_ _ _ -> return ()) (const $ const $ return ())
