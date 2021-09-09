{-# LANGUAGE DataKinds                     #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE GeneralisedNewtypeDeriving    #-}
{-# LANGUAGE TypeApplications              #-}

module Raft.Log (
    LogTerm
    , LogIndex
    , Log(..)
    , LogEntry(..)
    , mkLogTerm
    , mkLogIndex
    , checkAppendEntries
    , appendEntries
    , clearStaleEntriesAndAppend
    , logLastTerm
    , logTermAtIndex
    , logLength
    , logIndexForLength
    , logIndexToVectorIndex
    , nextLogIndex
    , startLogTerm
    , startLogIndex
    , incrementLogIndex
    , incrementLogTerm
) where

import qualified Data.Vector as V
import Data.Vector ((!?))
import GHC.Generics (Generic)
import GHC.Records (HasField(..))

-- | Useful log types are below. We want to disambiguate Terms from Indices
-- Because they're both integers.

-- | Every log entry has a term, which is the era in which it was produced.
-- Log terms are incremented only by candidates.
newtype LogTerm = LTerm { unLogTerm :: Int }  deriving (Show, Eq, Generic, Ord, Num)
-- | Every log entry has an index which represents its location in the log
-- Note: the Raft paper uses 1-based indexing, which this codebase follows!
newtype LogIndex = LIndex { unLogIndex :: Int } deriving (Show, Eq, Generic, Ord, Num)

-- | In order to make sure that these are non-negative, we
-- have constructor functions for them. This is meant to
-- keep us honest. The data constructors are not exported.
mkLogTerm :: Int -> Maybe LogTerm
mkLogTerm n
    | n < 0 = Nothing
    | otherwise = Just (LTerm n)

mkLogIndex :: Int -> Maybe LogIndex
mkLogIndex n
    | n < 0 = Nothing
    | otherwise = Just (LIndex n)

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
-- Note: we generally don't provide access to the underlying constructors
-- so that we can preserve our rule that indexes/terms are >= 0.

-- | Append Entries is one of the most important parts of raft. We start with a function to ascertain
-- whether append entries is allowed for this log.
checkAppendEntries :: Log a -> LogIndex -> LogTerm -> Bool
checkAppendEntries log' prevIndex prevTerm
    | prevIndex == startLogIndex = True
    | prevIndex >= LIndex (logLength log' + 1) = False
    | prevIndex >= 1 && (logTermAtIndex log' prevIndex /= prevTerm) = False
    | otherwise = True

-- | Append Entries is the heart of managing Raft's Log, which is the core
-- data structure that Raft nodes are working to achieve consensus around.
-- From the Raft paper §5.3: "If an existing entry conflicts with a new one
-- (same index, but different terms), delete the existing entry and all that follow it."
appendEntries :: Log a -> V.Vector (LogEntry a) -> LogIndex -> LogTerm -> (Log a, Bool)
appendEntries log' newEntries prevIndex prevTerm =
    if not (checkAppendEntries log' prevIndex prevTerm)
        then (log', False)
        else
            let
                updatedEntries = clearStaleEntriesAndAppend prevIndex log' newEntries
            in (Log updatedEntries, True)

-- | This function checks for the non-matching term from the LogIndex forward
-- and it will _clear_ out all remaining entries if there is no match.
clearStaleEntriesAndAppend :: LogIndex -> Log a -> V.Vector (LogEntry a) -> V.Vector (LogEntry a)
clearStaleEntriesAndAppend entryIndex log' newEntries =
    let
        insertion = logIndexToVectorIndex entryIndex
        (entriesUpToIndex, prevTail) = V.splitAt insertion (entries log')

        prevTailWithIndex = V.indexed prevTail
        compareToNew (idx, oldItem) = (Just $ term oldItem) == (term <$> newEntries !? idx)
        (oldTailWithIndex, dropConflictWithIndex) = V.partition compareToNew prevTailWithIndex
        newEntriesSliced =
            if V.null oldTailWithIndex
            then
                newEntries
            else
                let (offset, _) = V.last oldTailWithIndex
                in V.drop offset newEntries
        oldTailPreserved = V.map snd oldTailWithIndex
    in V.concat[entriesUpToIndex, oldTailPreserved, newEntriesSliced]

-- | Utilities for slicing into the log to discover
--  terms, indices, etc.
-- Note: we follow the Raft paper here and use 1-based indexing!
-- | Retrieves the term of the last item in the log or 0 if log is empty.
logLastTerm :: Log a -> LogTerm
logLastTerm log'
    | logLength log' > 0 = (getField @"term") . V.last . entries $ log'
    | otherwise = startLogTerm

-- | Retrieves the term of an item at a specific index in the log or 0 if log doesn't include that index. Assumes `LogIndex` is using 1-based indexing!
logTermAtIndex :: Log a -> LogIndex -> LogTerm
logTermAtIndex log' (LIndex index)
    | index == 0 = startLogTerm
    | otherwise =
        case entries log' !? (index - 1) of
            Nothing -> startLogTerm
            Just entry -> getField @"term" entry

-- | Simple function to get the length of a log
logLength :: Log a -> Int
logLength = V.length . entries

-- | Calculate the index from the length
logIndexForLength :: Log a -> LogIndex
logIndexForLength = LIndex . logLength

-- | Next index for the log
nextLogIndex :: Log a -> LogIndex
nextLogIndex = incrementLogIndex . logIndexForLength

-- | Adding one to the LogTerm
incrementLogTerm :: LogTerm -> LogTerm
incrementLogTerm (LTerm n) = LTerm (n + 1)

-- | Adding one to the LogTerm
incrementLogIndex :: LogIndex -> LogIndex
incrementLogIndex (LIndex n) = LIndex (n + 1)

-- | Joke's on me with this one, isn't it?
logIndexToVectorIndex :: LogIndex -> Int
logIndexToVectorIndex (LIndex n) = n - 1

-- | Raft uses 1-based indexing, so we will _start_
-- all terms and indices at 0
startLogTerm :: LogTerm
startLogTerm = LTerm 0

startLogIndex :: LogIndex
startLogIndex = LIndex 0