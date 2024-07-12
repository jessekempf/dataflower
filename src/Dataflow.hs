{-# LANGUAGE RecordWildCards #-}

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
  Dataflow,
  Edge,
  Timestamp,
  send,
  vertex,
  Program,
  programLastTimestamp,
  compile,
  execute
) where

import           Control.Monad              (void)
import           Control.Monad.IO.Class     (MonadIO (..))
import           Data.Traversable           (Traversable)
import           Dataflow.Primitives
import Control.Monad.Reader (ReaderT(runReaderT))
import Data.IORef (newIORef, IORef)
import Debug.Trace (traceM)

-- | A 'Program' represents a fully-preprocessed 'Dataflow' that may be
-- executed against inputs.
--
-- @since 0.1.0.0
data Program i = Program {
  programInput :: Edge i,
  programLastEpoch :: Epoch,
  programState :: IORef DataflowState
}

-- | Take a 'Dataflow' which takes 'i's as input and compile it into a 'Program'.
--
-- @since 0.1.0.0
compile :: MonadIO io => Dataflow (Edge i) -> io (Program i)
compile (Dataflow actions) = liftIO $ do
  -- traceM "== COMPILING =="
  stateRef <- newIORef initDataflowState
  edge <- runReaderT actions stateRef
  return $ Program edge (Epoch 0) stateRef

-- | Feed a traversable collection of inputs to a 'Program'. All inputs provided will
-- have the same 'Timestamp' associated with them.
--
-- @since 0.1.0.0
execute :: (MonadIO io, Traversable t, Show (t i), Show i) => t i -> Program i -> io (Program i)
execute corpus Program{..} = liftIO $ do
  -- traceM ("== EXECUTING " ++ show timestamp ++ " ==")
  runReaderT (runDataflow $ input programInput timestamp corpus) programState

  return $ Program programInput epoch programState

  where
    epoch = inc programLastEpoch
    timestamp = Timestamp epoch

programLastTimestamp :: Program i -> Timestamp
programLastTimestamp Program{..} = Timestamp programLastEpoch