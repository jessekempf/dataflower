module Test.Dataflow
  ( runDataflow,
    runDataflowMany,
  )
where

import           Control.Concurrent          (threadDelay)
import           Control.Concurrent.STM.TVar (modifyTVar', newTVarIO, readTVar,
                                              readTVarIO)
import           Control.Monad               (foldM_)
import           Control.Monad.IO.Class      (MonadIO (..))
import           Control.Monad.Trans.Class   (lift)
import           Dataflow                    (Edge, compile, execute, VertexReference,
                                              programLastTimestamp, send, synchronize)
import           Dataflow.Primitives         (Dataflow (Dataflow), atomically,
                                               vertex, quiesce)
import           Debug.Trace                 (traceM, traceShowM)
import           Prelude
import Text.Printf (printf)

-- | Run a dataflow with a list of inputs. All inputs will be sent as part of
-- a single epoch.
--
-- @since 0.1.0.0
runDataflow :: (Eq o, Eq i, Show o, Show i, MonadIO io) => (VertexReference o -> Dataflow (VertexReference i)) -> [i] -> io [o]
runDataflow dataflow inputs = head <$> runDataflowMany dataflow [inputs]

-- | Run a dataflow with a list of lists of inputs. Each outer list will be
-- sent as its own epoch.
--
-- @since 0.2.2.0
runDataflowMany :: (Eq o, Eq i, Show o, Show i, MonadIO io) => (VertexReference o -> Dataflow (VertexReference i)) -> [[i]] -> io [[o]]
runDataflowMany dataflow inputs =
  liftIO $ do
    out <- newTVarIO []
    program <- compile (dataflow =<< outputTVarNestedList out)

    foldM_ (flip execute) program inputs

    synchronize program

    readTVarIO out
  where
    outputTVarNestedList register =
        vertex
        []
        (\_ x state -> do
          -- traceM (printf "adding %s to state %s -> %s" (show x) (show state) (show $ x : state))
          return (x : state))
        ( \_ state -> do
            -- traceM ("state to latch: " ++ show state)
            atomically $ modifyTVar' register (state :)
            return []
        )
