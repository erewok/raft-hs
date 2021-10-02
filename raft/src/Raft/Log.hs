{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

module Raft.Log (
    Log (..),
    LogEntries,
    LogEntry (..),
    LogIndex,
    LogTerm,
    appendEntries,
    checkAppendEntries,
    clearStaleEntriesAndAppend,
    getLogEntry,
    incrementLogIndex,
    incrementLogTerm,
    logIndexToInt,
    logIndexToVectorIndex,
    logIsBehindOrEqual,
    logLastIndex,
    logLastTerm,
    logTermAtIndex,
    mkLogIndex,
    mkLogTerm,
    nextLogIndex,
    slice,
    startLogIndex,
    startLogTerm,
) where

import Data.Text (Text)
import Data.Vector ((!?))
import qualified Data.Vector as V
import GHC.Generics (Generic)
import GHC.Records (HasField (..))

{- | This alias is for readability: a Log will hold a vector of LogEntries.
 Even though we can *index* into a vector, we mostly do not. The Log
 object contains an @offset@ which represents the fact that some or all of
 its entries may actually be on disk and some or none held in memory.
-}
type LogEntries a = V.Vector (LogEntry a)

{- | We want to disambiguate Terms from Indices because they're both unsigned integers.
 Every log entry has a term, which is the era in which it was produced.
 Log terms are non-negative integers, and may be incremented only by candidates.
 Note: we don't provide access to the underlying constructors
 so that we can preserve our rule that indexes/terms are >= 0.
-}
newtype LogTerm = LTerm
    {unLogTerm :: Int}
    deriving (Show, Eq, Generic, Ord, Num)

{- | Every log entry has a non-negative index which represents its location in the log
 Note: the Raft paper uses 1-based indexing, which this codebase follows!
 Also: we don't provide access to the underlying constructors
 so that we can preserve our rule that indexes/terms are >= 0.
-}
newtype LogIndex = LIndex
    {unLogIndex :: Int}
    deriving (Show, Eq, Generic, Ord, Num)

{- | A raft Log is a list of Log Entries.
 This will be the subject of our consensus.
-}
data Log a = Log
    { -- | All log entries are stored in here. Even though we put them in a vector,
      -- we will rely on the entry's index and term for comparing other entries. The offset
      -- will be the location in the full log where this in-memory vector _starts_.
      -- Important: this vector won't necessarily store ALL ITEMS in the entire log in memory!
      entries :: LogEntries a
    , -- Since some entries may be compacted or stored on disk (in "non-volatile storage")
      -- we need to know where in the complete log this log starts. For a new log
      -- the offset starts at 0 and the first entry is at Index 1.
      offset :: !LogIndex
    }
    deriving stock (Show, Eq, Generic)

-- | A LogEntry has a term and the contents of the entry.
data LogEntry a = LogEntry
    { -- | Every LogEntry has a term.
      term :: !LogTerm
    , -- | We record the index for each entry instead of relying
      -- on our parent data structure to record it.
      -- This simplifies comparisons; inspired by etcd implementation.
      index :: !LogIndex
    , -- | The content is arbitrary.
      content :: a
    }
    deriving stock (Show, Generic, Functor)

{- | Our Raft implementation must fulfill the "Log Matching Property" from Section §5.3.
 If two entries in different logs have the same index and term,
 then they store the same command. We do not compare content as a result.
-}
instance Eq (LogEntry a) where
    (==) l1 l2 = term l1 == term l2 && index l1 == index l2

{- | Functions to operate on logs.
 | Append Entries is the heart of managing Raft's Log, which is the core
 data structure that Raft nodes are working to achieve consensus around.
 From the Raft paper §5.3: "If an existing entry conflicts with a new one
 (same index, but different terms), delete the existing entry and all that follow it."
-}
appendEntries :: LogIndex -> LogTerm -> LogEntries a -> Log a -> (Log a, Bool)
appendEntries prevIndex prevTerm newEntries log'
    | checkAppendEntries prevIndex prevTerm log' = (clearStaleEntriesAndAppend (prevIndex + 1) newEntries log', True)
    | otherwise = (log', False)

-- | Ascertain whether append entries is allowed for this log.
checkAppendEntries :: LogIndex -> LogTerm -> Log a -> Bool
checkAppendEntries prevIndex prevTerm log'
    | prevIndex == startLogIndex = True
    | prevIndex >= nextLogIndex log' = False
    | prevIndex >= 1 && (logTermAtIndex prevIndex log' /= prevTerm) = False
    | otherwise = True

{- | This function checks for the non-matching term from the LogIndex forward
 nand it will _clear_ out all remaining entries if there is no match.
 TODO: This comment lies about the implementation.
-}
clearStaleEntriesAndAppend :: LogIndex -> LogEntries a -> Log a -> Log a
clearStaleEntriesAndAppend entryIndex newEntries log'
    -- Happy path: new entries can be added onto the end with no conflicts
    | entryIndex == nextLogIndex log' = log'{entries = V.concat [entries log', newEntries]}
    -- Conflict case 1: entire log is stale and must be cleared (may clear stored entries as well). Must reset offset!
    | entryIndex <= offset log' = log'{entries = newEntries, offset = entryIndex - 1}
    -- Conflict case 2: log is partially stale: slice existing good entries and append new
    | otherwise =
        case takeTo entryIndex log' of
            Left _ -> log'
            Right entriesTruncated -> log'{entries = V.concat [entriesTruncated, newEntries]}

-- | A LogIndex and Log a specialized version of `take`
takeTo :: LogIndex -> Log a -> Either Text (LogEntries a)
takeTo entryIndex log' =
    checkTakeBounds entryIndex log' >> Right truncatedVec
  where
    takeLen = logIndexToVectorIndex $ entryIndex - offset log'
    truncatedVec = V.take takeLen (entries log')

slice :: LogIndex -> LogIndex -> Log a -> LogEntries a
slice start len log'
    | V.null $ entries log' = V.empty
    | start > logLastIndex log' = V.empty
    | len <= 0 = V.empty
    | len > currentEntryCount log' = V.empty
    | otherwise = V.slice (logIndexToVectorIndex (start - (offset log'))) (logIndexToInt len) (entries log')

-- | Validate that the index specified for `takeTo` is valid.
checkTakeBounds :: LogIndex -> Log a -> Either Text ()
checkTakeBounds entryIndex log'
    | takeLen < 0 = Left "take length is negative"
    | takeLen > V.length (entries log') = Left "take length is longer than log"
    | otherwise = Right ()
  where
    takeLen = logIndexToInt $ entryIndex - offset log' - 1

{- | Utilities for indexing into the log to discover
  terms, indices, etc.
 Note: we follow the Raft paper here and use 1-based indexing!
 | Retrieves the term of the last item in the log or 0 if log is empty.
-}
logLastTerm :: Log a -> LogTerm
logLastTerm log'
    | logLastIndex log' > 0 = (getField @"term") . V.last . entries $ log'
    | otherwise = startLogTerm

-- | Retrieves the term of an item at a specific index in the log or 0 if log doesn't include that index. Assumes `LogIndex` is using 1-based indexing!
logTermAtIndex :: LogIndex -> Log a -> LogTerm
logTermAtIndex idx log' =
    case getLogEntry idx (entries log') of
        Nothing -> startLogTerm
        Just entry -> getField @"term" entry

-- | Get the length of a log using the offset
logLastIndex :: Log a -> LogIndex
logLastIndex log' = (+) (offset log') (currentEntryCount log')

currentEntryCount :: Log a -> LogIndex
currentEntryCount log' = LIndex . V.length . entries $ log'

{- | Given a Term and an Index, check if Log is more up-to-date than these.
 If the logs end with different terms, then the log ending on the
 later term is more up-to-date. If the logs end with the same term,
 then the log with the larger last index is more up-to-date.
 If the logs are the same, the log is up-to-date.
-}
logIsBehindOrEqual :: LogTerm -> LogIndex -> Log a -> Bool
logIsBehindOrEqual lterm idx log'
    | lterm > logLastTerm log' = True
    | lterm == logLastTerm log' && idx >= logLastIndex log' = True
    | otherwise = False

-- | Next index for the log
nextLogIndex :: Log a -> LogIndex
nextLogIndex = incrementLogIndex . logLastIndex

-- | Adding one to the LogTerm
incrementLogTerm :: LogTerm -> LogTerm
incrementLogTerm (LTerm n) = LTerm (n + 1)

-- | Adding one to the LogIndex
incrementLogIndex :: LogIndex -> LogIndex
incrementLogIndex (LIndex n) = LIndex (n + 1)

{- | Locating an entry in a log involves matching on the index
 There's a further complication we have added here where a LogEntry
 may _lie_ about its Index. That would present a problem.
-}
getLogEntry :: LogIndex -> LogEntries a -> Maybe (LogEntry a)
getLogEntry idx entries' =
    let entry = entries' !? logIndexToVectorIndex idx
        justIdx = index <$> entry
        result = if justIdx == Just idx then entry else Nothing
     in result

-- | Turning it back into a plain int
logIndexToInt :: LogIndex -> Int
logIndexToInt (LIndex n) = n

-- | Turning it back into a plain Vector index
logIndexToVectorIndex :: LogIndex -> Int
logIndexToVectorIndex idx = logIndexToInt idx - 1

{- | Raft uses 1-based indexing, so we will _start_
 all terms and indices at 0. These represent the Null states.
-}
startLogTerm :: LogTerm
startLogTerm = LTerm 0

startLogIndex :: LogIndex
startLogIndex = LIndex 0

{- | In order to make sure that terms and indexes are non-negative, we
 have constructor functions for them. This is meant to
 keep us honest. The data constructors are not exported.
-}
mkLogTerm :: Int -> Maybe LogTerm
mkLogTerm n
    | n < 0 = Nothing
    | otherwise = Just (LTerm n)

mkLogIndex :: Int -> Maybe LogIndex
mkLogIndex n
    | n < 0 = Nothing
    | otherwise = Just (LIndex n)

-- | These are non-negative ints, so the instances here are unsurprising
instance Semigroup LogIndex where
    (<>) (LIndex a) (LIndex b) = LIndex (a + b)

instance Semigroup LogTerm where
    (<>) (LTerm a) (LTerm b) = LTerm (a + b)

instance Monoid LogIndex where
    mempty = startLogIndex

instance Monoid LogTerm where
    mempty = startLogTerm

{- | Some useful log instances.
 This instance is dangerous: the log should always be in Index-ascneding order.
-}
instance Semigroup (Log a) where
    {-# INLINE (<>) #-}
    (<>) log1 log2 =
        Log
            { offset = offset log1 <> offset log2
            , entries = entries log1 <> entries log2
            }

instance Monoid (Log a) where
    {-# INLINE mempty #-}
    mempty = Log{entries = V.empty, offset = startLogIndex}

instance Functor Log where
    {-# INLINE fmap #-}
    fmap f log' = log'{entries = V.map (fmap f) (entries log')}
    {-# INLINE (<$) #-}
    (<$) val log' = log'{entries = V.map (fmap $ const val) (entries log')}

-- instance Foldable (Log a) where
--   {-# INLINE foldr #-}
--   foldr = V.foldr . entries
