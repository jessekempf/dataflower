module Test.Dataflow
  ( runDataflow,
    runDataflowMany,
  )
where

import           Control.Concurrent.STM.TVar (modifyTVar', newTVarIO,
                                              readTVarIO)
import           Control.Monad               (foldM_)
import           Control.Monad.IO.Class      (MonadIO (..))
import           Dataflow                    (compile, execute, VertexReference, synchronize, Dataflow)
import           Dataflow.Primitives         (atomically,
                                               vertex, Node (Node))
import           Prelude
import Control.Monad.Trans (MonadTrans(..))

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
        vertex []
        (\_ x state -> return (x : state))
        ( \_ state -> Node . lift $ modifyTVar' register (state :) >> return [])
