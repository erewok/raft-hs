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
    { currentTerm   :: !LogTerm
    , commitIndex   :: !LogIndex
    , lastApplied   :: !LogIndex
    , allServerIds  :: HS.HashSet ServerId
    , votedFor      :: !(Maybe ServerId)
    , currentLeader :: !(Maybe ServerId)
    } deriving (Show, Eq)

-- | CandidateState is the state recorded for each candidate
data CandidateState = CandidateState
    { currentTerm    :: !LogTerm
    , commitIndex    :: !LogIndex
    , lastApplied    :: !LogIndex
    , allServerIds   :: HS.HashSet ServerId
    , votesGranted   :: HS.HashSet ServerId
    , votesResponded :: HS.HashSet ServerId
    } deriving (Show, Eq)

-- | LeaderState is the state recorded for a leader.
--  There should only be one leader at a time in a cluster.
data LeaderState = LeaderState
    { currentTerm  :: !LogTerm
    , commitIndex  :: !LogIndex
    , lastApplied  :: !LogIndex
    , allServerIds :: HS.HashSet ServerId
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
            , allServerIds   = getField @"allServerIds" followerState
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

-- |  Here, we convert a server to a leader.
-- Only a Candidate may be converted
convertToLeader :: Server a -> Server a
convertToLeader (Candidate serverId candidateState log') =
    let
        serverIds = getField @"allServerIds" candidateState
        serverIdList = HS.toList serverIds
        newState = LeaderState {
            currentTerm    = getServerTerm candidateState
            , commitIndex  = getField @"commitIndex" candidateState
            , lastApplied  = getField @"lastApplied" candidateState
            , allServerIds = serverIds
            , nextIndex    = HM.fromList $ map (\s -> (s, nextLogIndex log')) serverIdList
            , matchIndex   = HM.fromList $ map (\s -> (s, startLogIndex)) serverIdList
        }
    in
        Leader serverId newState log'
convertToLeader server = server

-- | After a RequestVoteResponse has been returned, we need to evaluate whether
-- the Candidate has won, lost, or needs to keep waiting.
-- If the election timeout happens again, the Candidate may
-- call another election and convert itself to a *new* Candidate.
maybePromoteCandidate :: Server a -> Server a
maybePromoteCandidate server@(Candidate serverId candidateState log')
    | (hasQuorum respondedVotes) && (hasQuorum grantedVotes) = convertToLeader server
    | (hasQuorum respondedVotes) && (not . hasQuorum $ grantedVotes) = convertToFollower server
    | otherwise = server
    where
        quorum = calcQuorum (getField @"allServerIds" candidateState)
        hasQuorum n = n >= quorum
        respondedVotes = HS.size (votesResponded candidateState)
        grantedVotes = HS.size (votesGranted candidateState)

-- | Quorum is an important attribute for a Raft cluster.
-- It is a simple majority of an odd number of servers.
calcQuorum :: HS.HashSet ServerId -> Int
calcQuorum serverIds = (div (HS.size serverIds) 2) + 1

-- | Here we convert a server to a follower.
--  Only a Candidate or a Leader may be converted.
--  A Follower is returned unchanged.
convertToFollower :: Server a -> Server a
convertToFollower (Candidate serverId candidateState log') =
    let
        newState = FollowerState {
            currentTerm     = getServerTerm candidateState
            , commitIndex   = getField @"commitIndex" candidateState
            , lastApplied   = getField @"lastApplied" candidateState
            , allServerIds  = getField @"allServerIds" candidateState
            , votedFor      = Nothing
            , currentLeader = Nothing
        }
    in
        Follower serverId newState log'
convertToFollower (Leader serverId leaderState log') =
    let
        newState = FollowerState {
            currentTerm     = getServerTerm leaderState
            , commitIndex   = getField @"commitIndex" leaderState
            , lastApplied   = getField @"lastApplied" leaderState
            , allServerIds  = getField @"allServerIds" leaderState
            , votedFor      = Nothing
            , currentLeader = Nothing
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
        state' = if success' then state { votedFor = Nothing, currentLeader = Just $ getSource msg } else state
        server'@(Follower _ state'' _) = updateServerTerm msgTerm (Follower serverId state' updatedLog)
        response = if msgTermValid then Just $ mkAppendEntriesResponse success' msg state'' else Nothing
    in
        (response, server')
handleAppendEntries msg server@(Candidate _ state _)
    | getField @"term" msg > getServerTerm state = handleAppendEntries msg (convertToFollower server)
    | otherwise = (Nothing, server)
handleAppendEntries msg server@(Leader _ state _)
    | getField @"term" msg > getServerTerm state = handleAppendEntries msg (convertToFollower server)
    | otherwise = (Nothing, server)

-- | This function handles the request vote RPC. Followers can vote.
-- If a Candidate finds out that it's behind, it may convert to a follower and then vote.
handleRequestVote :: RequestVoteRPC -> Server a -> (Maybe RequestVoteResponseRPC, Server a)
handleRequestVote request server@(Follower serverId state serverLog) =
    let
        msgTerm = getTerm request
        msgTermValid = msgTerm >= getServerTerm state
        candidateIsUpToDate = logIsBehindOrEqual (lastLogTerm request) (lastLogIndex request) serverLog
        (voted, state') = if candidateIsUpToDate then voteForServer request state else (False, state)
        server' = updateServerTerm msgTerm (Follower serverId state' serverLog)
        response = mkRequestVoteResponse voted request server'
    in
        (Just response, server')
-- If it's a Candidate or Leader, it should *ignore* requests for votes unless it discovers
-- its term is out of date. In that case, it converts to a follower and then it may vote.
handleRequestVote request server@(Candidate _ state _)
    | lastLogTerm request > getServerTerm state = handleRequestVote request (convertToFollower server)
    | otherwise = (Just $ mkRequestVoteResponse False request server, server)
handleRequestVote request server@(Leader _ state _)
    | lastLogTerm request > getServerTerm state = handleRequestVote request (convertToFollower server)
    | otherwise = (Just $ mkRequestVoteResponse False request server, server)

-- | A Leader upon receiving an AppendEntriesResponseRPC must update its state
handleAppendEntriesResponse :: AppendEntriesResponseRPC -> Server a -> Server a
handleAppendEntriesResponse response server@(Leader serverId state serverLog) = undefined
handleAppendEntriesResponse response server = undefined

-- | A Candidate upon receiving a RequestVoteResponseRPC must update its state
-- If it wins an election, it should be converted to a leader.
handleRequestVoteResponse :: RequestVoteResponseRPC -> Server a -> Server a
handleRequestVoteResponse response server@(Candidate serverId state serverLog) =
    let
        respondent = getSource response
        state' = state { votesResponded = HS.insert respondent (votesResponded state)}
        state'' =
            if voteGranted response
            then state' { votesGranted = HS.insert respondent (votesGranted state') }
            else state'
    in
        maybePromoteCandidate (Candidate serverId state'' serverLog)
handleRequestVoteResponse response server = server

-- | Functions to generate RPCs.
-- Leaders generate AppendEntriesRPCs.
generateAppendEntriesRPC :: ServerId -> LeaderState -> Log a -> AppendEntriesRPC a
generateAppendEntriesRPC recvr state log' = undefined

generateAppendEntriesRPCList :: Server a -> [AppendEntriesRPC a]
generateAppendEntriesRPCList server@(Leader serverId state log') = undefined -- don't send to self!
generateAppendEntriesRPCList server = []


-- | Candidates generate RequestVoteRPCs.
generateRequestVoteRPC :: ServerId -> CandidateState -> Log a -> RequestVoteRPC
generateRequestVoteRPC recvr state log' = undefined

generateRequestVoteRPCList :: Server a -> [RequestVoteRPC]
generateRequestVoteRPCList server@(Candidate serverId state log') = undefined  -- don't send to self!
generateRequestVoteRPCList server = []


-- | Any message received from a server with a term greater than this server's
-- means this server must *immediately* update its term to the latest.
-- This comes from Raft paper §...
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

-- | Candidates or Followers will need to *increment* their term when an election begins
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