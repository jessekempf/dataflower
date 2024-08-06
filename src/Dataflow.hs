{-# LANGUAGE DataKinds                 #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE ImpredicativeTypes        #-}
{-# LANGUAGE KindSignatures            #-}
{-# LANGUAGE LambdaCase                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE ScopedTypeVariables       #-}

{-|
Module      : Dataflow
Description : Timely Dataflow for Haskell
Copyright   : (c) Double Crown Gaming Co. 2020
License     : BSD3
Maintainer  : jesse.kempf@doublecrown.co
Stability   : experimental

Timely Dataflow in pure Haskell.
-}

module Dataflow (
  Graph,
  Node,
  Edge,
  Timestamp,
  send,
  vertex,
  inputVertex,
  Vertex,
  using,
  Program,
  Phase(..),
  prepare,
  start,
  stop,
  submit,
  synchronize,
  output,
) where

import           Control.Concurrent     (forkIO)
import           Control.Concurrent.STM (STM, TMVar, TQueue, TVar, atomically,
                                         check, flushTQueue, isEmptyTQueue,
                                         modifyTVar', newEmptyTMVarIO,
                                         newTVarIO, orElse, putTMVar, readTVar,
                                         readTVarIO, retry, takeTMVar,
                                         writeTQueue, writeTVar)
import           Control.DeepSeq        (NFData (..))
import           Control.Monad          (forM, forM_, unless, void, when)
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.State    (runStateT)
import           Control.Monad.Trans    (MonadTrans (..))
import           Data.Map               (Map, adjust, minViewWithKey)
import qualified Data.Map               as Map
import qualified Data.Map.Strict
import           Data.Vector            (Vector, map, (!))
import           Dataflow.Primitives
import           GHC.Natural            (Natural)
import           Prelude                hiding ((<>))
import           Unsafe.Coerce          (unsafeCoerce)

data RunState =  Run | Stop deriving (Eq, Show)

data Phase = Prepared | Running | Stopped deriving Show

-- | A 'Program' represents a fully-preprocessed 'Dataflow' that may be
-- executed against inputs.
--
-- @since 0.1.0.0
data Program (phase :: Phase) i = Program {
  programInput     :: TQueue (Timestamp, i),
  programNextEpoch :: Epoch,
  programContent   :: Vector ProgramVertex
}

instance NFData (Program 'Running i) where
  rnf = const ()

data ProgramVertex = forall i. ProgramVertex {
  pvVertexDef          :: VertexDef i,
  pvRunState           :: TVar RunState,
  pvExited             :: TMVar (),
  pvTimestampProducers :: TVar (Map Timestamp Natural),
  pvInputCount         :: Natural
}

-- | Take a 'Dataflow' which takes 'i's as input and compile it into a 'Program'.
--
-- @since 0.1.0.0
prepare :: MonadIO io => Graph (Input i) -> io (Program 'Prepared i)
prepare (Graph actions) = liftIO $ do
  (Input input, graph) <- runStateT actions initDataflowState

  programContent <- forM (dfsVertices graph) $ \vertexDef -> do
    runState <- liftIO $ newTVarIO Run
    exited <- liftIO newEmptyTMVarIO
    timestampProducers <- liftIO $ newTVarIO Map.empty

    return ProgramVertex {
      pvVertexDef = vertexDef,
      pvRunState = runState,
      pvExited = exited,
      pvTimestampProducers = timestampProducers,
      pvInputCount = fromIntegral $ length (vertexDefInputs vertexDef)
    }

  return Program {
    programInput = input,
    programNextEpoch = Epoch 0,
    programContent = programContent
  }

start :: MonadIO io => Program 'Prepared i -> io (Program 'Running i)
start Program{..} = liftIO $ do
  programContent' <- forM programContent $ \ProgramVertex{..} -> do
    void $ case pvVertexDef of
      VertexDef{..} ->
        forkIO $ do
          -- Runloop code
          while pvRunState (== Run) $ runNode dfg $ do
              inputs <- Node . lift $ do
                flushTQueue vertexDefInputQueue >>= \case
                  [] -> retry
                  q -> return q

              forM_ inputs $ \(ts, i) ->
                runStatefully vertexDefStateRef $ vertexDefOnSend ts i
            <> do
              timestamp <- Node . lift $ do
                producersTable <- readTVar pvTimestampProducers
                case minViewWithKey producersTable of
                  Just ((timestamp, 0), producers') -> do
                    writeTVar pvTimestampProducers producers'
                    return timestamp
                  _ -> retry

              runStatefully vertexDefStateRef $ vertexDefOnNotify timestamp

              forM_ vertexDefOutputs $ \(VertexID index) -> do
                let ProgramVertex { pvTimestampProducers = ovTimestampProducers } = unsafeCoerce (programContent ! index)
                Node . lift $ modifyTVar' ovTimestampProducers (adjust (\x -> x - 1) timestamp)
          -- Shutdown code
          atomically $ putTMVar pvExited ()

    return ProgramVertex{..}

  return Program{programContent = programContent', ..}

  where
    while :: TVar a -> (a -> Bool) -> STM () -> IO ()
    while stateVar predicate action = do
      condition <- predicate <$> readTVarIO stateVar
      when condition $ do
        atomically $ action `orElse` ((check . (/= condition)) . predicate =<< readTVar stateVar)
        while stateVar predicate action

    dfg :: DataflowGraph
    dfg = DataflowGraph $ Data.Vector.map (\ProgramVertex{..} -> unsafeCoerce pvVertexDef) programContent


synchronize :: MonadIO io => Program 'Running i -> io (Program 'Running i)
synchronize Program{..} = do
  liftIO $ atomically $
    forM_ programContent $ \ProgramVertex{..} -> do
      check =<< isEmptyTQueue programInput
      check . Data.Map.Strict.null =<< readTVar pvTimestampProducers

  return Program{..}

stop :: MonadIO io => Program 'Running i -> io (Program 'Stopped i)
stop Program{..} = do
  liftIO . atomically $ do
    -- Wait for the computation graph to be drained fully
    forM_ programContent $ \ProgramVertex{..} -> do
      check =<< isEmptyTQueue programInput
      check . Data.Map.Strict.null =<< readTVar pvTimestampProducers

  liftIO . atomically $ do
    -- Tell all running vertex threads to stop
    forM_ programContent $ \ProgramVertex{..} ->
      writeTVar pvRunState Stop

  -- Wait for each and every thread to have stopped
  forM_ programContent $ \ProgramVertex{..} -> liftIO . atomically $ takeTMVar pvExited

  return Program{..}

-- | Feed a traversable collection of inputs to a 'Program'. All inputs provided will
-- have the same 'Timestamp' associated with them.
--
-- @since 0.1.0.0
submit :: (MonadIO io, Traversable t, Show (t i), Show i, Eq i) => t i -> Program 'Running i -> io (Program 'Running i)
submit corpus Program{..} = liftIO $ do
  let timestamp = Timestamp programNextEpoch

  forM_ corpus $ \item -> atomically $ do
    writeTQueue programInput (timestamp, item)

  atomically $ do
    forM_ programContent $ \ProgramVertex{..} -> do
      timestampProducers <- readTVar pvTimestampProducers

      unless (timestamp `Data.Map.Strict.member` timestampProducers) $ modifyTVar' pvTimestampProducers (Data.Map.Strict.insert timestamp pvInputCount)

  return Program{programNextEpoch = inc programNextEpoch, ..}
