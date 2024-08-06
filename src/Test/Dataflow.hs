module Test.Dataflow
  ( runDataflow,
    runDataflowMany,
  )
where

import           Control.Concurrent.STM.TVar (modifyTVar', newTVarIO,
                                              readTVarIO)
import           Control.Monad               (foldM, void)
import           Control.Monad.IO.Class      (MonadIO (..))
import           Control.Monad.Trans         (MonadTrans (..))
import           Dataflow                    (Graph, Vertex, inputVertex,
                                              prepare, start, stop, submit)
import           Dataflow.Operators          (statefulVertex)
import           Dataflow.Primitives         (Node (Node))
import           Prelude

-- | Run a dataflow with a list of inputs. All inputs will be sent as part of
-- a single epoch.
--
-- @since 0.1.0.0
runDataflow :: (Eq o, Eq i, Show o, Show i, MonadIO io) => (Vertex o -> Graph (Vertex i)) -> [i] -> io [o]
runDataflow dataflow inputs = head <$> runDataflowMany dataflow [inputs]

-- | Run a dataflow with a list of lists of inputs. Each outer list will be
-- sent as its own epoch.
--
-- @since 0.2.2.0
runDataflowMany :: (Eq o, Eq i, Show o, Show i, MonadIO io) => (Vertex o -> Graph (Vertex i)) -> [[i]] -> io [[o]]
runDataflowMany dataflow inputs =
  liftIO $ do
    out <- newTVarIO []
    runnableProgram <- prepare (inputVertex . dataflow =<< outputTVarNestedList out)
    program <- start runnableProgram

    void $ stop =<< foldM (flip submit) program inputs

    reverse <$> readTVarIO out
  where
    outputTVarNestedList register =
        statefulVertex []
        (\_ x state -> return (x : state))
        ( \_ state -> Node . lift $ modifyTVar' register (state :))

