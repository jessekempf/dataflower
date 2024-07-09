module Test.Dataflow
  ( runDataflow,
    runDataflowMany,
    bypass2,
    bypass3,
  )
where

import Control.Concurrent.STM.TVar
  ( modifyTVar',
    newTVarIO,
    readTVarIO,
  )
import Control.Monad (foldM_)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.STM (atomically)
import Control.Monad.Trans.Class (lift)
import Dataflow
  ( Edge,
    compile,
    execute,
    finalize,
    modifyState,
    readState,
    send,
    statefulVertex,
    statelessVertex,
    writeState,
  )
import Dataflow.Primitives (Dataflow (Dataflow))
import Prelude

-- | Run a dataflow with a list of inputs. All inputs will be sent as part of
-- a single epoch.
--
-- @since 0.1.0.0
runDataflow :: MonadIO io => (Edge o -> Dataflow (Edge i)) -> [i] -> io [o]
runDataflow dataflow inputs = head <$> runDataflowMany dataflow [inputs]

-- | Run a dataflow with a list of lists of inputs. Each outer list will be
-- sent as its own epoch.
--
-- @since 0.2.2.0
runDataflowMany :: MonadIO io => (Edge o -> Dataflow (Edge i)) -> [[i]] -> io [[o]]
runDataflowMany dataflow inputs =
  liftIO $ do
    out <- newTVarIO []
    program <- compile (dataflow =<< outputTVarNestedList out)

    foldM_ (flip execute) program inputs

    reverse <$> readTVarIO out
  where
    outputTVarNestedList register =
      statefulVertex
        []
        (\sref _ x -> modifyState sref (x :))
        ( \sref _ -> do
            state <- readState sref

            Dataflow $ lift $ atomically $ modifyTVar' register (reverse state :)

            writeState sref []
        )

bypass2 :: Edge a -> Edge b -> Dataflow (Edge a)
bypass2 next other =
  statelessVertex
    (send next)
    (\timestamp -> finalize next timestamp >> finalize other timestamp)

bypass3 :: Edge a -> Edge b -> Edge c -> Dataflow (Edge a)
bypass3 next other1 other2 =
  statelessVertex
    (send next)
    (\timestamp -> finalize next timestamp >> finalize other1 timestamp >> finalize other2 timestamp)