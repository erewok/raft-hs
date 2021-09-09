{-# LANGUAGE OverloadedStrings #-}

module Raft.LogSpec where

import Control.Monad (join)
import Data.Maybe (fromJust)
import Data.Text (Text)
import qualified Data.Vector as V
import Test.Hspec
import Test.Hspec.QuickCheck (prop)

import Raft.Log
import SpecFixtures

checkAppendEntriesConsistencyLengthProp :: (Eq a) => V.Vector (LogEntry a) -> V.Vector (LogEntry a) -> Bool
checkAppendEntriesConsistencyLengthProp entries1 entries2 = V.length entries1 >= V.length entries2

checkAppendEntriesTailConsistencyProp :: (Eq a) => LogIndex -> V.Vector (LogEntry a) -> V.Vector (LogEntry a) -> Bool
checkAppendEntriesTailConsistencyProp lgIdx entries1 entries2 =
  V.drop (logIndexToVectorIndex lgIdx) entries1 == entries2


logSpec :: Spec
logSpec = do
  describe "incrementLogIndex" $ do
    prop "isomoprhic logIndexToVectorIndex with incrementLogIndex" $
        let maybeN n = logIndexToVectorIndex . incrementLogIndex <$> mkLogIndex n
        in \n -> if n >= 0 then maybeN n == Just n else maybeN n == Nothing
    prop "incrementLogIndex is LogIndex +1" $
      let maybeN n = incrementLogIndex <$> mkLogIndex n
      in \n -> if n >= 0 then maybeN n == mkLogIndex (n + 1) else maybeN n == Nothing
  describe "incrementLogTerm" $ do
    prop "incrementLogIndex is LogIndex +1" $
      let maybeN n = incrementLogTerm <$> mkLogTerm n
      in \n -> if n >= 0 then maybeN n == mkLogTerm (n + 1) else maybeN n == Nothing
  -- Section §5.3: Log Matching Property:
  -- If two entries in different logs have the same index and term,
  --   then they store the same command
  -- If two entries in different logs have the same index and term,
  --   then the logs are identical in all preceding entries
  -- We will assert these rules are maintained after invoking our functions
  describe "Section §5.3: Log Matching Property with clearStaleEntriesAndAppend" $ do
    prop "Consistent after clearStaleEntriesAndAppend: Log length >= new entries length" $
      \log1 entries -> do
        idx <- arbitraryLogIndex entries
        let clearedEntries = clearStaleEntriesAndAppend idx log1 (entries :: V.Vector (LogEntry Bool))
        pure $ checkAppendEntriesConsistencyLengthProp clearedEntries entries
    prop "Consistency after clearStaleEntriesAndAppend: newEntries == Tail of Existing" $
      \log1 entries -> do
        idx <- arbitraryLogIndex entries
        let clearedEntries = clearStaleEntriesAndAppend idx log1 (entries :: V.Vector (LogEntry Bool))
        pure $ checkAppendEntriesTailConsistencyProp idx clearedEntries entries
