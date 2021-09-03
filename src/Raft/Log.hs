{-# LANGUAGE DataKinds                     #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE GeneralisedNewtypeDeriving    #-}
{-# LANGUAGE TypeApplications              #-}

module Raft.Log where

import qualified Data.Vector as V
import Data.Vector ((!?))
import GHC.Generics (Generic)
import GHC.Records (HasField(..))

-- | Useful log types are below. We want to disambiguate Terms from Indices
-- Because they're both integers.

-- | Every log entry has a term, which is the era in which it was produced.
-- Log terms are incremented only by candidates.
newtype LogTerm = LogTerm { unLogTerm :: Int }  deriving (Show, Eq, Generic, Ord, Num)
-- | Every log entry has an index which represents its location in the log
-- Note: the Raft paper uses 1-based indexing, which this codebase follows!
newtype LogIndex = LogIndex { unLogIndex :: Int } deriving (Show, Eq, Generic, Ord, Num)

-- | A raft Log is a list of Log Entries.
-- This will be the subject of our consensus.
newtype Log a = Log
    {
    -- | All log entries are in stored in here
    entries :: V.Vector (LogEntry a)
    }  deriving (Show, Eq, Generic)

-- | A LogEntry has a term and the contents of the entry.
data LogEntry a = LogEntry
    { term :: !LogTerm
    , content :: a
    } deriving (Show, Eq, Generic)


-- | Functions to operate on logs.
-- We are sticking to the 1-based indexing used in the Raft paper.

-- | Append Entries is one of the most important parts of raft. We start with a function to ascertain whether append entries is allowed for this log
checkAppendEntries :: Log a -> LogIndex -> LogTerm -> Bool
checkAppendEntries log' prevIndex prevTerm
    | prevIndex >= LogIndex (logLength log' + 1) = False
    | prevIndex >= 1 && (logTermAtIndex log' prevIndex /= prevTerm) = False
    | otherwise = True

-- | Append Entries is the heart of managing Raft's Log, which is the core
-- data structure that Raft nodes are working to achieve consensus around.
-- From the paper: "If an existing entry conflicts with a new one
-- (same index, but different terms), delete the existing entry and all that follow it."
appendEntries :: Log a -> V.Vector (LogEntry a) -> LogIndex -> LogTerm -> (Log a, Bool)
appendEntries log' newEntries prevIndex prevTerm =
    if not (checkAppendEntries log' prevIndex prevTerm)
        then (log', False)
        else
            let
                clearedVec = clearStaleEntries prevIndex log' newEntries
                updatedLog = Log $ clearedVec V.++  newEntries
            in (updatedLog, True)

-- | This function checks for the non-matching term from the LogIndex forward
-- and it will _clear_ out all remaining entries if there is no match.
clearStaleEntries :: LogIndex -> Log a -> V.Vector (LogEntry a) -> V.Vector (LogEntry a)
clearStaleEntries entryIndex log' newEntries =
    let
        insertion = (unLogIndex entryIndex)
        prevEntries = entries log'
        headVec = V.take insertion prevEntries
        prevTail = V.drop insertion prevEntries
        newTail = V.map (\(l, _) -> l) $ V.takeWhile (\(left, right) -> (term left == term right)) $ V.zip prevTail newEntries
    in headVec V.++ newTail

-- | Utilities for slicing into the log to discover
--  terms, indices, etc.
-- Note: we follow the Raft paper here and use 1-based indexing!
-- | Retrieves the term of the last item in the log or 0 if log is empty.
logLastTerm :: Log a -> LogTerm
logLastTerm log' =
    if logLength log' > 0
    then (getField @"term") . V.last . entries $ log'
    else LogTerm 0

-- | Retrieves the term of an item at a specific index in the log or 0 if log doesn't include that index. Assumes `LogIndex` is using 1-based indexing!
logTermAtIndex :: Log a -> LogIndex -> LogTerm
logTermAtIndex log' (LogIndex index) =
    case entries log' !? (index - 1) of
        Nothing -> LogTerm 0
        Just entry -> getField @"term" entry

-- | Simple function to get the length of a log
logLength :: Log a -> Int
logLength = V.length . entries