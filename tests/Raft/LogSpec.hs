module Raft.LogSpec where

import Control.Monad (join)
import Test.Hspec
import Test.Hspec.QuickCheck (prop)

import Raft.Log

logSpec :: Spec
logSpec = do
  describe "incrementLogIndex" $ do
    prop "isomoprhic logIndexToVectorIndex with incrementLogIndex" $
        let maybeN n = logIndexToVectorIndex . incrementLogIndex <$> mkLogIndex n
        in \n ->  if n >= 0 then maybeN n == Just n else maybeN n == Nothing
