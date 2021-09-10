{-# LANGUAGE OverloadedStrings #-}

module Raft.LogSpec where

import Control.Monad (join)
import Data.Maybe (fromJust)
import Data.Text (Text)
import qualified Data.Vector as V
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck (Property, ioProperty, property, (===))

import Raft.Log
import SpecFixtures

prop_clearStaleEntriesEmptyLog :: V.Vector (LogEntry Bool) -> Property
prop_clearStaleEntriesEmptyLog entries' = do
    let clearedEntries = clearStaleEntriesAndAppend startLogIndex (Log {entries = V.empty}) entries'
    property $ clearedEntries === entries'

prop_clearStaleEntriesLengthConsistent :: ArbLogWithIndex Bool -> V.Vector (LogEntry Bool) -> Property
prop_clearStaleEntriesLengthConsistent arbLgIdx entries' = do
    let idx = lgIndex arbLgIdx
        log' = lg arbLgIdx
        clearedEntries = clearStaleEntriesAndAppend idx log' entries'
    property $ (V.length clearedEntries) >= V.length entries'

prop_clearStaleEntriesTailConsistent :: ArbLogWithIndex Bool -> V.Vector (LogEntry Bool) -> Property
prop_clearStaleEntriesTailConsistent arbLgIdx entries' = do
  let idx = lgIndex arbLgIdx
      log' = lg arbLgIdx
      clearedEntries = clearStaleEntriesAndAppend idx log' entries'
  case idx of
    startLogIndex -> clearedEntries === entries'
    _ -> (V.drop (logIndexToVectorIndex idx) clearedEntries) === entries'


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
    prop "incrementLogTerm is LogTerm +1" $
      let maybeN n = incrementLogTerm <$> mkLogTerm n
      in \n -> if n >= 0 then maybeN n == mkLogTerm (n + 1) else maybeN n == Nothing
  -- Section §5.3: Log Matching Property:
  -- If two entries in different logs have the same index and term,
  --   then they store the same command
  -- If two entries in different logs have the same index and term,
  --   then the logs are identical in all preceding entries
  -- We will assert these rules are maintained after invoking our functions
  describe "Section §5.3: Log Matching Property with clearStaleEntriesAndAppend: " $ do

    -- I dont think this test makes any sense. TODO: Rethink this.
    -- it "Should handle empty entries" $ property prop_clearStaleEntriesEmptyEntries

    it "Should handle an empty log to start" $ property prop_clearStaleEntriesEmptyLog

    it "Log length >= new entries length" $ property prop_clearStaleEntriesLengthConsistent

    it "newEntries == Tail of Existing" $ property prop_clearStaleEntriesTailConsistent