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
import GHC.Generics (Generic)


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
    -- | All log entries are in here
    entries :: [LogEntry a]
    }  deriving (Show, Eq, Generic)

-- | A LogEntry has a term and the contents of the entry.
data LogEntry a = LogEntry
    { term :: LogTerm
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

-- | A raft server may only be one of three different types.
data Server a where
    Follower    :: ServerId -> FollowerState  -> Log a -> Server a
    Candidate   :: ServerId -> CandidateState -> Log a -> Server a
    Leader      :: ServerId -> LeaderState    -> Log a -> Server a

-- | A moment in time is a simulated clock.
--  A Heartbeat occurs after so many ticks.
--  An ElectionTimeout occurs after so many ticks _without_ a Heartbeat Received.
--  If we fold over a moment in time comprised of so many ticks or ElectionTimeouts without
--  seeing a heartbeat, then we have an election timeout.
data Moment =
    Tick
    | Heartbeat
    | ElectionTimeout

-- | FollowerState is the state recorded for each follower
data FollowerState = FollowerState
    { currentTerm   :: !LogTerm
    , fCommitIndex  :: !LogIndex
    , fLastApplied  :: !LogIndex
    , votedFor      :: !(Maybe ServerId)
    } deriving (Show, Eq)

-- | CandidateState is the state recorded for each candidate
data CandidateState = CandidateState
    { candidateTerm  :: !LogTerm
    , cCommitIndex   :: !LogIndex
    , cLastApplied   :: !LogIndex
    , votesGranted   :: HS.HashSet ServerId
    , votesResponded :: HS.HashSet ServerId
    } deriving (Show, Eq)

-- | LeaderState is the state recorded for a leader.
--  There should only be one leader at a time in a cluster.
data LeaderState = LeaderState
    { leaderTerm    :: !LogTerm
    , lCommitIndex  :: !LogIndex
    , lLastApplied  :: !LogIndex
    , matchIndex    :: HM.HashMap ServerId LogIndex
    , nextIndex     :: HM.HashMap ServerId LogIndex
    } deriving (Show, Eq)

-- | Raft defines the following RPC types:
--  Append Entries request, append Entries response,
--  request vote, and request vote response.
data AppendEntriesRPC a = AppendEntriesRPC
    { sourceAndDest :: !SourceDest
    , prevLogIndex  :: !LogIndex
    , prevLogTerm   :: !LogTerm
    , logEntries    :: [LogEntry a]
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

data RequestVoteResponseRPC = ResponseVoteRPC
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
convertToCandidate (Follower serverId followerState log') =
    let
        votesResponded' = HS.fromList [ serverId ]
        votesGranted' = HS.fromList [ serverId ]
        newTerm  = LogTerm $ ((+1) . unLogTerm . currentTerm ) followerState
        newState = CandidateState {
            candidateTerm = newTerm,
            cCommitIndex = fCommitIndex followerState,
            cLastApplied = fLastApplied followerState,
            votesResponded = votesResponded',
            votesGranted = votesGranted'
        }
    in
        Candidate serverId newState log'
convertToCandidate (Candidate serverId candidateState log') =
    let
        newState = candidateState { candidateTerm = LogTerm $ ((+1) . unLogTerm . candidateTerm ) candidateState }
    in
        Candidate serverId newState log'
convertToCandidate leader@Leader {} = leader  -- Leaders cannot be converted into candidates

-- |  Here, we convert a server to a leader. Only a Candidate may be converted
convertToLeader :: [ServerId] -> Server a -> Server a
convertToLeader allServers (Candidate serverId candidateState log') =
    let
        logLength = length . entries $ log'
        newState = LeaderState {
            leaderTerm = candidateTerm candidateState
            , lCommitIndex = cCommitIndex candidateState
            , lLastApplied = cLastApplied candidateState
            , nextIndex = HM.fromList $ map (\s -> (s, LogIndex $ logLength + 1)) allServers
            , matchIndex = HM.fromList $ map (\s -> (s, LogIndex 0)) allServers
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
            currentTerm = candidateTerm candidateState
            , votedFor = Nothing
            , fCommitIndex = cCommitIndex candidateState
            , fLastApplied = cLastApplied candidateState
        }
    in
        Follower serverId newState log'
convertToFollower (Leader serverId leaderState log') =
    let
        newState = FollowerState {
            currentTerm = leaderTerm leaderState
            , votedFor = Nothing
            , fCommitIndex = lCommitIndex leaderState
            , fLastApplied = lLastApplied leaderState
        }
    in
        Follower serverId newState log'
convertToFollower follower = follower


-- | Receipt of RPCs means some state change and possibly a server change.
--  Here, we handle the append entries RPC
handleAppendEntries :: AppendEntriesRPC a -> Server a -> (AppendEntriesResponseRPC, Server a)
handleAppendEntries = undefined

-- | This function handles the request vote RPC.
handleRequestVote :: RequestVoteRPC -> Server a -> (RequestVoteResponseRPC, Server a)
handleRequestVote request server = undefined

-- | Servers will be translating a request into a response
mkAppendEntriesResponse :: AppendEntriesRPC a -> Server a -> AppendEntriesResponseRPC
mkAppendEntriesResponse request server =
    let
        senderRecvr = sourceAndDest @AppendEntriesRPC request
        sourceDest' = swapSourceAndDest senderRecvr
        resp = AppendEntriesResponseRPC {
            sourceAndDest = sourceDest
            , matchIndex = commitIndex request
            , success = True
            , term = 0
        }
    in resp

mkRequestVoteResponse :: RequestVoteRPC -> Server a -> RequestVoteResponseRPC
mkRequestVoteResponse request server = undefined

-- | When replying to an RPC, we swap the source and dest
--  So that the receiver knows where it needs to get returned to.
swapSourceAndDest :: SourceDest -> SourceDest
swapSourceAndDest input = input {
    source = dest input
    , dest = source input
    }
