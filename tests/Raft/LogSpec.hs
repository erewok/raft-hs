{-# LANGUAGE OverloadedStrings #-}

module Raft.LogSpec where

import Control.Monad (join)
import Data.Maybe (fromJust)
import Data.Text (Text)
import qualified Data.Vector as V
import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck (Property, counterexample, property, (===), (.||.))

import Raft.Log
import SpecFixtures


prop_clearStaleEntriesEmptyLog :: V.Vector (LogEntry Bool) -> Property
prop_clearStaleEntriesEmptyLog entries' = do
    let
      emptyLog = Log {entries = V.empty, offset = startLogIndex}
      clearedEntries = clearStaleEntriesAndAppend (incrementLogIndex startLogIndex) entries' emptyLog
    property $ (entries clearedEntries) === entries'

-- | The resulting Vector _should not be_ shorter than `newEntries` passed in
prop_clearStaleEntriesLengthConsistent :: ArbLogWithIndex Bool -> V.Vector (LogEntry Bool) -> Property
prop_clearStaleEntriesLengthConsistent arbLgIdx newEntries = do
    let idx = lgIndex arbLgIdx
        log' = lg arbLgIdx
        origEntries = entries log'
        clearedEntries = entries $ clearStaleEntriesAndAppend idx newEntries log'
        newTotalLength = V.length clearedEntries
        addedEntriesLength = V.length newEntries
        propCounterEx = counterexample $ mconcat [
          "\nCleared Entries Length: \t"
          , (show newTotalLength)
          , "\n New Entries Input Length: \t"
          , (show addedEntriesLength)
          , "\nCleared Entries Value: \t"
          , (show clearedEntries)
          ]
    -- no change scenario: when the indexes don't match up
    propCounterEx .  property $ (origEntries == clearedEntries) || (newTotalLength >= addedEntriesLength)

prop_clearStaleEntriesTailConsistent :: ArbLogWithIndex Bool -> V.Vector (LogEntry Bool) -> Property
prop_clearStaleEntriesTailConsistent arbLgIdx newEntries = do
  let idx = lgIndex arbLgIdx
      log' = lg arbLgIdx
      origEntries = entries log'
      setEntriesLog = V.imap (\ix e -> e { index = idx + (fromJust . mkLogIndex $ ix) }) newEntries
      takeLen = logIndexToInt $ idx - 1 - (offset log')
      clearedEntries = entries $ clearStaleEntriesAndAppend idx setEntriesLog log'
      newTotalLength = V.length clearedEntries
      clearedTail = V.drop takeLen clearedEntries
      propCounterEx = counterexample $ mconcat [
          "\nEntry Index: \t"
          , (show $ logIndexToInt idx)
          , "\nLog Offset: \t"
          , (show $ offset log')
          , "\nCleared Entries Length: \t"
          , (show newTotalLength)
          , "\nTaking the first " ++ show takeLen ++ " Entries"
          , "\nPutting these ones in: \t"
          , (show setEntriesLog)
          , "\nCleared Entries Value: \t"
          , (show clearedEntries)
          ]
  propCounterEx $
    if takeLen <= 0
      then clearedEntries === setEntriesLog
      else clearedTail === setEntriesLog


logSpec :: Spec
logSpec = do
  describe "incrementLogIndex" $ do
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

    it "Should handle an empty log to start" $ property prop_clearStaleEntriesEmptyLog

    it "Log length >= new entries length" $ property prop_clearStaleEntriesLengthConsistent

    it "newEntries == tail of existing log" $ property prop_clearStaleEntriesTailConsistent