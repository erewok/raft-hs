{-# LANGUAGE DataKinds                     #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE DeriveFoldable                #-}
{-# LANGUAGE DisambiguateRecordFields      #-}
{-# LANGUAGE DuplicateRecordFields         #-}
{-# LANGUAGE GADTs                         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving    #-}
{-# LANGUAGE OverloadedLabels              #-}
{-# LANGUAGE TypeApplications              #-}

-- |
-- Module      :  Raft
-- Copyright   :  (c) Erik Aker 2021
-- License     :  BSD-3
--
-- Maintainer  :  eraker@gmail.com
-- Stability   :  experimental
--
-- An implementation of Raft according to the TLA+ spec.  See
--
--  * The Formal TLA+ specification for the Raft consensus algorithm.
--    <https://github.com/ongardie/raft.tla/blob/master/raft.tla>
--
--
module Raft where

import Data.Hashable (Hashable)
import qualified Data.HashSet as HS
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import Data.Vector ((!?))
import GHC.Generics (Generic)
import GHC.Records (HasField(..))

-- | Every raft server has an Id, which is represented as an `int` here
newtype ServerId = ServerId { unServerId :: Int } deriving (Show, Eq, Generic)
instance Hashable ServerId

-- | Every log entry has a term, which is the era in which it was produced.
-- Log terms are incremented only by candidates.
newtype LogTerm = LogTerm { unLogTerm :: Int }  deriving (Show, Eq, Generic)
-- | Every log entry has an index which represents its location in the log
newtype LogIndex = LogIndex { unLogIndex :: Int } deriving (Show, Eq, Generic)

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

-- | There are four message types in raft (not including requests from clients)
data Msg a =
    AppendEntriesRequest    (AppendEntriesRPC a)
    | AppendEntriesResponse AppendEntriesResponseRPC
    | VoteRequest           RequestVoteRPC
    | VoteResponse          RequestVoteResponseRPC

-- | Events may trigger state changes or requests.
data Event a =
    TimeEvent Moment
    | MsgRecv (Msg a)
    | Restart

-- | A moment in time is a simulated clock.
--  A Heartbeat occurs after so many ticks.
--  An ElectionTimeout occurs after so many ticks _without_ a Heartbeat Received.
--  If we fold over a moment in time comprised of so many ticks or ElectionTimeouts without
--  seeing a heartbeat, then we have an election timeout.
data Moment =
    Tick
    | Heartbeat
    | ElectionTimeout


-- | A raft server may only be one of three different types.
data Server a where
    Follower    :: ServerId -> FollowerState  -> Log a -> Server a
    Candidate   :: ServerId -> CandidateState -> Log a -> Server a
    Leader      :: ServerId -> LeaderState    -> Log a -> Server a


-- | FollowerState is the state recorded for each follower
data FollowerState = FollowerState
    { term         :: !LogTerm
    , commitIndex  :: !LogIndex
    , lastApplied  :: !LogIndex
    , votedFor     :: !(Maybe ServerId)
    } deriving (Show, Eq)

-- | CandidateState is the state recorded for each candidate
data CandidateState = CandidateState
    { term           :: !LogTerm
    , commitIndex    :: !LogIndex
    , lastApplied    :: !LogIndex
    , votesGranted   :: HS.HashSet ServerId
    , votesResponded :: HS.HashSet ServerId
    } deriving (Show, Eq)

-- | LeaderState is the state recorded for a leader.
--  There should only be one leader at a time in a cluster.
data LeaderState = LeaderState
    { term         :: !LogTerm
    , commitIndex  :: !LogIndex
    , lastApplied  :: !LogIndex
    , matchIndex   :: HM.HashMap ServerId LogIndex
    , nextIndex    :: HM.HashMap ServerId LogIndex
    } deriving (Show, Eq)

-- | Raft defines the following RPC types:
--  Append Entries request, append Entries response,
--  request vote, and request vote response.
data AppendEntriesRPC a = AppendEntriesRPC
    { sourceAndDest :: !SourceDest
    , prevLogIndex  :: !LogIndex
    , prevLogTerm   :: !LogTerm
    , logEntries    :: V.Vector (LogEntry a)
    , commitIndex   :: !LogIndex
    }  deriving (Show, Eq)

data AppendEntriesResponseRPC = AppendEntriesResponseRPC
    { sourceAndDest :: !SourceDest
    , matchIndex    :: !LogIndex
    , success       :: !Bool
    , term          :: !LogTerm
    } deriving (Show, Eq)

data RequestVoteRPC = RequestVoteRPC
    { sourceAndDest :: !SourceDest
    , lastLogTerm   :: !LogTerm
    , lastLogIndex  :: !LogIndex
    , term          :: !LogTerm
    }

data RequestVoteResponseRPC = RequestVoteResponseRPC
    { sourceAndDest :: !SourceDest
    , voteGranted   :: !Bool
    , term          :: !LogTerm
    }  deriving (Show, Eq)

data SourceDest = SourceDest
    {
    source :: !ServerId
    , dest :: !ServerId
    } deriving (Show, Eq, Generic)

-- | Outbound communication with other nodes using channels
--  If the channel is full we _flush_ the channel and start adding values
--  The idea is that raft should be resilient enough that messages may be lost
--  and the server will merely try again later.


--  Fold over stream of incoming events and check that not too many ticks have
--  come through without a heartbeat received.


-- | State transitions from one server to another
--  A Candidate may become a Candidate, a Follower, or a Leader
--  A Follower may become a Candidate.
--  A Leader may become a Follower.
--  First, we define the state transition *to* a candidate.
convertToCandidate :: Server a -> Server a
convertToCandidate server@(Follower serverId followerState log') =
    let
        votesResponded' = HS.fromList [ serverId ]
        votesGranted' = HS.fromList [ serverId ]
        newTerm  = incrementLogTerm server
        newState = CandidateState {
            term             = newTerm
            , commitIndex    = getField @"commitIndex" followerState
            , lastApplied    = getField @"lastApplied" followerState
            , votesResponded = votesResponded'
            , votesGranted   = votesGranted'
        }
    in
        Candidate serverId newState log'
convertToCandidate server@(Candidate serverId candidateState log') =
    let
        newState = candidateState { term = incrementLogTerm server } :: CandidateState
    in
        Candidate serverId newState log'
convertToCandidate server = server  -- Leaders cannot be converted into candidates

-- |  Here, we convert a server to a leader. Only a Candidate may be converted
convertToLeader :: [ServerId] -> Server a -> Server a
convertToLeader allServers (Candidate serverId candidateState log') =
    let
        logLength = V.length . entries $ log'
        newState = LeaderState {
            term          = getField @"term" candidateState
            , commitIndex = getField @"commitIndex" candidateState
            , lastApplied = getField @"lastApplied" candidateState
            , nextIndex   = HM.fromList $ map (\s -> (s, LogIndex $ logLength + 1)) allServers
            , matchIndex  = HM.fromList $ map (\s -> (s, LogIndex 0)) allServers
        }
    in
        Leader serverId newState log'
convertToLeader _ server = server

-- | Here we convert a server to a follower.
--  Only a Candidate or a Leader may be converted.
--  A follower is return unchanged.
convertToFollower :: Server a -> Server a
convertToFollower (Candidate serverId candidateState log') =
    let
        newState = FollowerState {
            term          = getField @"term" candidateState
            , commitIndex = getField @"commitIndex" candidateState
            , lastApplied = getField @"lastApplied" candidateState
            , votedFor    = Nothing
        }
    in
        Follower serverId newState log'
convertToFollower (Leader serverId leaderState log') =
    let
        newState = FollowerState {
            term          = getField @"term" leaderState
            , votedFor    = Nothing
            , commitIndex = getField @"commitIndex" leaderState
            , lastApplied = getField @"lastApplied" leaderState
        }
    in
        Follower serverId newState log'
convertToFollower follower = follower


-- | Functions to generate RPCs:
-- Candidates generate RequestVoteRPCs.
-- Leaders generate AppendEntriesRPCs.
generateAppendEntriesRPC :: [ServerId] -> ServerId -> LeaderState -> Log a -> AppendEntriesRPC a
generateAppendEntriesRPC serverList leaderId state log' = undefined

generateRequestVoteRPC :: [ServerId] -> ServerId -> CandidateState -> Log a -> RequestVoteRPC
generateRequestVoteRPC serverList candidateId state log' = undefined

-- | Receipt of RPCs means some state change and possibly a server change.
--  Here, we handle the append entries RPC
handleAppendEntries :: AppendEntriesRPC a -> Server a -> (AppendEntriesResponseRPC, Server a)
handleAppendEntries = undefined

-- | This function handles the request vote RPC.
handleRequestVote :: RequestVoteRPC -> Server a -> (RequestVoteResponseRPC, Server a)
handleRequestVote request server = undefined

-- | Here servers will translate a request into a response
mkAppendEntriesResponse :: Bool -> AppendEntriesRPC a -> FollowerState -> AppendEntriesResponseRPC
mkAppendEntriesResponse successful request state =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = AppendEntriesResponseRPC {
            sourceAndDest = senderRecvr
            , matchIndex = getField @"commitIndex" request
            , success = successful
            , term = getField @"term" state
        }
    in resp

mkRequestVoteResponse :: Bool -> RequestVoteRPC -> Server a -> RequestVoteResponseRPC
mkRequestVoteResponse granted request (Follower _ state _) =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = RequestVoteResponseRPC {
            sourceAndDest = senderRecvr
            , voteGranted = granted
            , term = getField @"term" state
        }
    in resp
mkRequestVoteResponse granted request (Candidate _ state _) =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = RequestVoteResponseRPC {
            sourceAndDest = senderRecvr
            , voteGranted = granted
            , term = getField @"term" state
        }
    in resp
mkRequestVoteResponse granted request (Leader _ state _) =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = RequestVoteResponseRPC {
            sourceAndDest = senderRecvr
            , voteGranted = granted
            , term = getField @"term" state
        }
    in resp

-- | When replying to an RPC, we swap the source and dest
--  So that the receiver knows where it needs to get returned to.
swapSourceAndDest :: SourceDest -> SourceDest
swapSourceAndDest input = input {
    source = dest input
    , dest = source input
    }


-- | Utilities for slicing into the log to discover
--  terms, indices, etc. This is useful for all server types.
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


-- | Utilities for working with servers
incrementLogTerm :: Server a -> LogTerm
incrementLogTerm (Follower _ state _) = LogTerm $ ((+1) . unLogTerm . (getField @"term")) state
incrementLogTerm (Candidate _ state _) = LogTerm $ ((+1) . unLogTerm . (getField @"term")) state
-- Leader's log term must not be incremented
incrementLogTerm (Leader _ state _) = getField @"term" state