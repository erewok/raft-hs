{-# LANGUAGE DataKinds                     #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE DeriveFoldable                #-}
{-# LANGUAGE DuplicateRecordFields         #-}
{-# LANGUAGE GADTs                         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving    #-}
{-# LANGUAGE OverloadedLabels              #-}
{-# LANGUAGE TypeApplications              #-}


module Raft.Server where

import Data.Hashable (Hashable)
import qualified Data.HashSet as HS
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as V
import Data.Vector ((!?))
import GHC.Generics (Generic)
import GHC.Records (HasField(..))

import Raft.Log


-- | Every raft server has an Id, which is represented as an `int` here
newtype ServerId = ServerId { unServerId :: Int } deriving (Show, Eq, Generic)
instance Hashable ServerId

-- | A raft server may only be one of three different types.
data Server a where
    Follower    :: ServerId -> FollowerState  -> Log a -> Server a
    Candidate   :: ServerId -> CandidateState -> Log a -> Server a
    Leader      :: ServerId -> LeaderState    -> Log a -> Server a

-- | FollowerState is the state recorded for each follower
data FollowerState = FollowerState
    { currentTerm  :: !LogTerm
    , commitIndex  :: !LogIndex
    , lastApplied  :: !LogIndex
    , votedFor     :: !(Maybe ServerId)
    } deriving (Show, Eq)

-- | CandidateState is the state recorded for each candidate
data CandidateState = CandidateState
    { currentTerm  :: !LogTerm
    , commitIndex    :: !LogIndex
    , lastApplied    :: !LogIndex
    , votesGranted   :: HS.HashSet ServerId
    , votesResponded :: HS.HashSet ServerId
    } deriving (Show, Eq)

-- | LeaderState is the state recorded for a leader.
--  There should only be one leader at a time in a cluster.
data LeaderState = LeaderState
    { currentTerm  :: !LogTerm
    , commitIndex  :: !LogIndex
    , lastApplied  :: !LogIndex
    , matchIndex   :: HM.HashMap ServerId LogIndex
    , nextIndex    :: HM.HashMap ServerId LogIndex
    } deriving (Show, Eq)


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
        newTerm  = incrementServerLogTerm server
        newState = CandidateState {
            currentTerm      = newTerm
            , commitIndex    = getField @"commitIndex" followerState
            , lastApplied    = getField @"lastApplied" followerState
            , votesResponded = votesResponded'
            , votesGranted   = votesGranted'
        }
    in
        Candidate serverId newState log'
convertToCandidate server@(Candidate serverId candidateState log') =
    let
        newState = candidateState { currentTerm = incrementServerLogTerm server } :: CandidateState
    in
        Candidate serverId newState log'
convertToCandidate server = server  -- Leaders cannot be converted into candidates

-- |  Here, we convert a server to a leader. Only a Candidate may be converted
convertToLeader :: [ServerId] -> Server a -> Server a
convertToLeader allServers (Candidate serverId candidateState log') =
    let
        newState = LeaderState {
            currentTerm   = getField @"currentTerm" candidateState
            , commitIndex = getField @"commitIndex" candidateState
            , lastApplied = getField @"lastApplied" candidateState
            , nextIndex   = HM.fromList $ map (\s -> (s, nextLogIndex log')) allServers
            , matchIndex  = HM.fromList $ map (\s -> (s, startLogIndex)) allServers
        }
    in
        Leader serverId newState log'
convertToLeader _ server = server

-- | Here we convert a server to a follower.
--  Only a Candidate or a Leader may be converted.
--  A follower is returned unchanged.
convertToFollower :: Server a -> Server a
convertToFollower (Candidate serverId candidateState log') =
    let
        newState = FollowerState {
            currentTerm   = getField @"currentTerm" candidateState
            , commitIndex = getField @"commitIndex" candidateState
            , lastApplied = getField @"lastApplied" candidateState
            , votedFor    = Nothing
        }
    in
        Follower serverId newState log'
convertToFollower (Leader serverId leaderState log') =
    let
        newState = FollowerState {
            currentTerm   = getField @"currentTerm" leaderState
            , commitIndex = getField @"commitIndex" leaderState
            , lastApplied = getField @"lastApplied" leaderState
            , votedFor    = Nothing
        }
    in
        Follower serverId newState log'
convertToFollower follower = follower

-- | Utilities for working with servers
incrementServerLogTerm :: Server a -> LogTerm
incrementServerLogTerm (Follower _ state _) = incrementLogTerm . (getField @"currentTerm") $ state
incrementServerLogTerm (Candidate _ state _) = incrementLogTerm . (getField @"currentTerm") $ state
-- Leader's log term must not be incremented
incrementServerLogTerm (Leader _ state _) = getField @"currentTerm" state

-- | Reset vote for Follower only
resetFollowerVotedFor :: FollowerState -> FollowerState
resetFollowerVotedFor state = state { votedFor = Nothing }
