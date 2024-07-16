{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}

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
  VertexReference,
  connect,
  Program,
  programLastTimestamp,
  compile,
  execute,
  synchronize,
  outputSTM,
) where

import           Control.Monad              (void)
import           Control.Monad.IO.Class     (MonadIO (..))
import           Data.Traversable           (Traversable)
import           Dataflow.Primitives
import Control.Monad.Reader (ReaderT(runReaderT),)
import Data.IORef (newIORef, IORef)
import Debug.Trace (traceM)
import Control.Concurrent.STM (STM)
import qualified Data.Map.Strict
import Text.Printf (printf)

-- | A 'Program' represents a fully-preprocessed 'Dataflow' that may be
-- executed against inputs.
--
-- @since 0.1.0.0
data Program i = Program {
  programInput :: VertexReference i,
  programLastEpoch :: Epoch,
  programState :: IORef DataflowState
}

-- | Take a 'Dataflow' which takes 'i's as input and compile it into a 'Program'.
--
-- @since 0.1.0.0
compile :: MonadIO io => Dataflow (VertexReference i) -> io (Program i)
compile (Dataflow actions) = liftIO $ do
  -- traceM "== COMPILING =="
  stateRef <- newIORef initDataflowState
  edge <- runReaderT actions stateRef
  return $ Program edge (Epoch 0) stateRef

-- | Feed a traversable collection of inputs to a 'Program'. All inputs provided will
-- have the same 'Timestamp' associated with them.
--
-- @since 0.1.0.0
execute :: (MonadIO io, Traversable t, Show (t i), Show i, Eq i) => t i -> Program i -> io (Program i)
execute corpus Program{..} = liftIO $ do
  -- traceM ("== EXECUTING " ++ show timestamp ++ " ==")
  runReaderT (runDataflow $ input programInput timestamp corpus) programState

  return $ Program programInput epoch programState

  where
    epoch = inc programLastEpoch
    timestamp = Timestamp epoch

synchronize :: MonadIO io => Program i -> io ()
synchronize program = liftIO $ runReaderT (runDataflow quiesce) (programState program)

programLastTimestamp :: Program i -> Timestamp
programLastTimestamp Program{..} = Timestamp programLastEpoch

outputSTM :: (Eq o, Show o) => ([o] -> STM ()) -> Dataflow (VertexReference o)
outputSTM stmAction =
  vertex (Data.Map.Strict.empty :: Data.Map.Strict.Map Timestamp [o])
  (\timestamp o state ->
    return $ Data.Map.Strict.alter (\case
                                      Nothing -> Just [o]
                                      Just accum -> Just (o : accum)
                                    ) timestamp state
  )(\timestamp state -> do
      -- traceM $ printf "%s -> %s" (show timestamp) (show state)
      atomically (stmAction $ state Data.Map.Strict.! timestamp)
      -- traceM $ printf "outputSTM: ran stmAction(%s)" (show $ state Data.Map.Strict.! timestamp)
      return $ Data.Map.Strict.delete timestamp state
  )