{-# LANGUAGE DataKinds                     #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE DeriveFoldable                #-}
{-# LANGUAGE DuplicateRecordFields         #-}
{-# LANGUAGE FlexibleContexts              #-}
{-# LANGUAGE GADTs                         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving    #-}
{-# LANGUAGE OverloadedLabels              #-}
{-# LANGUAGE ScopedTypeVariables           #-}
{-# LANGUAGE TypeApplications              #-}


module Raft.Server where

import Data.Hashable (Hashable)
import qualified Data.HashSet as HS
import qualified Data.HashMap.Strict as HM
import Data.Maybe (isNothing)
import qualified Data.Vector as V
import Data.Vector ((!?))
import GHC.Generics (Generic)
import GHC.Records (HasField(..))

import Raft.Log
import Raft.Message
import Raft.Shared


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

-- | Conversion Functions: State transitions from one server to another
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
            currentTerm   = getServerTerm candidateState
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
            currentTerm   = getServerTerm candidateState
            , commitIndex = getField @"commitIndex" candidateState
            , lastApplied = getField @"lastApplied" candidateState
            , votedFor    = Nothing
        }
    in
        Follower serverId newState log'
convertToFollower (Leader serverId leaderState log') =
    let
        newState = FollowerState {
            currentTerm   = getServerTerm leaderState
            , commitIndex = getField @"commitIndex" leaderState
            , lastApplied = getField @"lastApplied" leaderState
            , votedFor    = Nothing
        }
    in
        Follower serverId newState log'
convertToFollower follower = follower

-- | Receipt of RPCs means some state change and possibly a server change.
--  Here, we handle the append entries RPC which should be sent by the supposed leader
-- to all other servers.
-- Note: only a Follower will update its log in response to the AppendEntriesRPC.
handleAppendEntries :: AppendEntriesRPC a -> Server a -> (Maybe AppendEntriesResponseRPC, Server a)
handleAppendEntries msg server@(Follower serverId state serverLog) =
    let
        msgTerm = getField @"term" msg
        msgTermValid = msgTerm >= getServerTerm state
        success = msgTermValid && appendReqCheckLogOk msg serverLog
        (updatedLog, success') = appendEntries (prevLogIndex msg) (prevLogTerm msg) (logEntries msg)  serverLog
        -- Make sure to reset any previous votedFor status
        state' = if success' then resetFollowerVotedFor state else state
        server'@(Follower _ state'' _) = updateServerTerm msgTerm (Follower serverId state' updatedLog)
        response = if msgTermValid then Just $ mkAppendEntriesResponse success' msg state'' else Nothing
    in
        (response, server')
handleAppendEntries msg server@(Candidate serverId state serverLog) =
    let
        msgTerm = getField @"term" msg
        msgTermValid = msgTerm >= getServerTerm state
        server'@(Candidate _ state' _) = updateServerTerm msgTerm (Candidate serverId state serverLog)
    in
        if msgTermValid
            then
                let
                    follower@(Follower _ state'' _) = convertToFollower server'
                    response = Just $ mkAppendEntriesResponse False msg state''
                in
                    (response, follower)
            else
                (Nothing, server')
handleAppendEntries msg server@(Leader serverId state serverLog) =
    let
        msgTerm = getTerm msg
        msgTermValid = msgTerm > getServerTerm state
        server' = updateServerTerm msgTerm $ if msgTermValid then convertToFollower server else server
    in
        (Nothing, server')

-- | This function handles the request vote RPC.
handleRequestVote :: RequestVoteRPC -> Server a -> (Maybe RequestVoteResponseRPC, Server a)
handleRequestVote request server@(Follower serverId state serverLog) =
    let
        msgTerm = getTerm request
        msgTermValid = msgTerm >= getServerTerm state
        (voted, state') = voteForServer request state
        server' = updateServerTerm msgTerm server
        response = mkRequestVoteResponse voted request server'
    in
        (Just response, server')
handleRequestVote request server@(Candidate serverId state serverLog) =
    let
        msgTerm = getTerm request
        msgTermValid = msgTerm > getServerTerm state
        server' = updateServerTerm msgTerm $ if msgTermValid then convertToFollower server else server
        response = mkRequestVoteResponse False request server'
    in
        (Just response, server')
handleRequestVote request server@(Leader serverId state serverLog) =
    let
        msgTerm = getTerm request
        msgTermValid = msgTerm > getServerTerm state
        server' = updateServerTerm msgTerm $ if msgTermValid then convertToFollower server else server
        response = mkRequestVoteResponse False request server'
    in
        (Just response, server')

-- | A Leader upon receiving an AppendEntriesResponseRPC must update its state
handleAppendEntriesResponse :: AppendEntriesResponseRPC -> Server a -> Server a
handleAppendEntriesResponse response server@(Leader serverId state serverLog) = undefined
handleAppendEntriesResponse response server = undefined

-- | A Candidate upon receiving a RequestVoteResponseRPC must update its state
handleRequestVoteResponse :: RequestVoteResponseRPC -> Server a -> Server a
handleRequestVoteResponse response server@(Candidate serverId state serverLog) = undefined
handleRequestVoteResponse response server = server


-- | Here servers will translate a request into a response
-- | Only a follower is *allowed* to reply to an AppendEntriesRPC
mkAppendEntriesResponse :: Bool -> AppendEntriesRPC a -> FollowerState -> AppendEntriesResponseRPC
mkAppendEntriesResponse successful request state =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = AppendEntriesResponseRPC {
            sourceAndDest = senderRecvr
            , matchIndex = getField @"commitIndex" request
            , success = successful
            , term = getServerTerm state
        }
    in resp

-- | Request vote responses may be handled by any server type
mkRequestVoteResponse :: Bool -> RequestVoteRPC -> Server a -> RequestVoteResponseRPC
mkRequestVoteResponse granted request (Follower _ state _) =
    RequestVoteResponseRPC {
            sourceAndDest = senderRecvr
            , voteGranted = granted
            , term = getField @"currentTerm" state
        }
    where
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
mkRequestVoteResponse granted request (Candidate _ state _) =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = RequestVoteResponseRPC {
            sourceAndDest = senderRecvr
            , voteGranted = granted
            , term = getField @"currentTerm" state
        }
    in resp
mkRequestVoteResponse granted request (Leader _ state _) =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = RequestVoteResponseRPC {
            sourceAndDest = senderRecvr
            , voteGranted = granted
            , term = getField @"currentTerm" state
        }
    in resp

-- | Functions to generate RPCs:
-- Candidates generate RequestVoteRPCs.
-- Leaders generate AppendEntriesRPCs.
generateAppendEntriesRPC :: [ServerId] -> ServerId -> LeaderState -> Log a -> AppendEntriesRPC a
generateAppendEntriesRPC serverList leaderId state log' = undefined

generateRequestVoteRPC :: [ServerId] -> ServerId -> CandidateState -> Log a -> RequestVoteRPC
generateRequestVoteRPC serverList candidateId state log' = undefined

-- | Utilities for working with servers

-- | Any message received from a server with a term greater than this server's
-- means this server must *immediately* update its term to the latest
updateServerTerm :: LogTerm -> Server a -> Server a
updateServerTerm t (Follower serverId serverState log') = Follower serverId state' log'
    where
        state' = if t > (getServerTerm serverState)
            then serverState { currentTerm = t } :: FollowerState
            else serverState

updateServerTerm t (Candidate serverId serverState log') = Candidate serverId state' log'
    where
        state' = if t > (getServerTerm serverState)
            then serverState { currentTerm = t } :: CandidateState
            else serverState

updateServerTerm t (Leader serverId serverState log') = Leader serverId state' log'
    where
        state' = if t > (getServerTerm serverState)
            then serverState { currentTerm = t } :: LeaderState
            else serverState

-- | Candidates or Followers may need to *increment* their term when an election begins
incrementServerLogTerm :: Server a -> LogTerm
incrementServerLogTerm (Follower _ state _) = incrementLogTerm . getServerTerm $ state
incrementServerLogTerm (Candidate _ state _) = incrementLogTerm . getServerTerm $ state
-- Leader's log term must not be incremented: they do not start elections.
incrementServerLogTerm (Leader _ state _) = getServerTerm state

-- | We pull this term out a lot, so this alias is useful
getServerTerm :: HasField "currentTerm" r LogTerm => r -> LogTerm
getServerTerm r = getField @"currentTerm" r

-- | Reset vote for Follower only
resetFollowerVotedFor :: FollowerState -> FollowerState
resetFollowerVotedFor state = state { votedFor = Nothing }

-- | Check if voting possible and if so, modify `votedFor` on FollowerState
voteForServer :: RequestVoteRPC -> FollowerState -> (Bool, FollowerState)
voteForServer request state
    | msgTerm >= serverTerm && hasntVotedYet = (True, state { votedFor = votesFor})
    | otherwise = (False, state)
    where
        msgTerm = getTerm request
        serverTerm = getServerTerm state
        hasntVotedYet = isNothing . votedFor $ state
        votesFor = Just $ getSource request

-- | Check append entries request has expected log. Does this duplicate the one in Raft.Log
appendReqCheckLogOk :: AppendEntriesRPC a -> Log a -> Bool
appendReqCheckLogOk req log'
    | prevIndex == startLogIndex = True
    | otherwise =
         prevIndex > startLogIndex
            && prevIndex <= nextLogIndex log'
                && prevLogTerm req == logTermAtIndex prevIndex log'
    where prevIndex = prevLogIndex req