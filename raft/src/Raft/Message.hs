{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TypeApplications #-}

module Raft.Message where

import qualified Data.Vector as V
import GHC.Records ( HasField(..) )

import Raft.Log ( LogEntry, LogIndex, LogTerm )
import Raft.Shared ( ServerId )

{- | Raft defines the following RPC types:
  Append Entries request, append Entries response,
  request vote, and request vote response.
 Each of these will include the source and destination
 as well as the term, which is something we learn from the TLA+ spec.
-}
data Msg a
    = AppendEntriesRequest (AppendEntriesRPC a)
    | AppendEntriesResponse AppendEntriesResponseRPC
    | VoteRequest RequestVoteRPC
    | VoteResponse RequestVoteResponseRPC

data AppendEntriesRPC a = AppendEntriesRPC
    { sourceAndDest :: !SourceDest
    , prevLogIndex :: !LogIndex
    , prevLogTerm :: !LogTerm
    , logEntries :: V.Vector (LogEntry a)
    , commitIndex :: !LogIndex
    , term :: !LogTerm
    }
    deriving (Show, Eq)

data AppendEntriesResponseRPC = AppendEntriesResponseRPC
    { sourceAndDest :: !SourceDest
    , matchIndex :: !LogIndex
    , success :: !Bool
    , term :: !LogTerm
    }
    deriving (Show, Eq)

data RequestVoteRPC = RequestVoteRPC
    { sourceAndDest :: !SourceDest
    , lastLogTerm :: !LogTerm
    , lastLogIndex :: !LogIndex
    , term :: !LogTerm
    }
    deriving (Show, Eq)

data RequestVoteResponseRPC = RequestVoteResponseRPC
    { sourceAndDest :: !SourceDest
    , voteGranted :: !Bool
    , term :: !LogTerm
    }
    deriving (Show, Eq)

data SourceDest = SourceDest
    { source :: !ServerId
    , dest :: !ServerId
    }
    deriving (Show, Eq)

{- | When replying to an RPC, we swap the source and dest
  So that the receiver knows where it needs to get returned to.
-}
swapSourceAndDest :: SourceDest -> SourceDest
swapSourceAndDest input =
    input
        { source = dest input
        , dest = source input
        }

-- | We pull these terms out a lot, so these aliases are useful
getTerm :: HasField "term" r LogTerm => r -> LogTerm
getTerm = getField @"term"

getSourceAndDest :: HasField "sourceAndDest" r SourceDest => r -> SourceDest
getSourceAndDest = getField @"sourceAndDest"

getSource :: HasField "sourceAndDest" r SourceDest => r -> ServerId
getSource = source . getSourceAndDest

getDest :: HasField "sourceAndDest" r SourceDest => r -> ServerId
getDest = dest . getSourceAndDest

staleTerm :: HasField "term" r LogTerm => r -> LogTerm -> Bool
staleTerm r cmpTerm = getField @"term" r < cmpTerm