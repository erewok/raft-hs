{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE TypeApplications #-}

module Raft.Server where

import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import Data.List (sort)
import Data.Maybe (isNothing, mapMaybe)
import GHC.Records (HasField (..))

import qualified Raft.Log as RL
import qualified Raft.Message as RM
import Raft.Shared (ServerId)


-- | A raft server may only be one of three different types.
data Server a =
    Follower ServerId FollowerState (RL.Log a)
    | Candidate ServerId CandidateState (RL.Log a)
    | Leader ServerId LeaderState (RL.Log a)
    deriving (Eq, Show)

-- | FollowerState is the state recorded for each follower
data FollowerState = FollowerState
    { currentTerm :: !RL.LogTerm
    , commitIndex :: !RL.LogIndex
    , lastApplied :: !RL.LogIndex
    , allServerIds :: HS.HashSet ServerId
    , votedFor :: !(Maybe ServerId)
    , currentLeader :: !(Maybe ServerId)
    }
    deriving (Show, Eq)

-- | CandidateState is the state recorded for each candidate
data CandidateState = CandidateState
    { currentTerm :: !RL.LogTerm
    , commitIndex :: !RL.LogIndex
    , lastApplied :: !RL.LogIndex
    , allServerIds :: HS.HashSet ServerId
    , votesGranted :: HS.HashSet ServerId
    , votesResponded :: HS.HashSet ServerId
    }
    deriving (Show, Eq)

{- | LeaderState is the state recorded for a leader.
  There should only be one leader at a time in a cluster.
-}
data LeaderState = LeaderState
    { currentTerm :: !RL.LogTerm
    , commitIndex :: !RL.LogIndex
    , lastApplied :: !RL.LogIndex
    , allServerIds :: HS.HashSet ServerId
    , matchIndex :: HM.HashMap ServerId RL.LogIndex
    , nextIndex :: HM.HashMap ServerId RL.LogIndex
    }
    deriving (Show, Eq)

{- | Conversion Functions: State transitions from one server to another
  A Candidate may become a Candidate, a Follower, or a Leader
  A Follower may become a Candidate.
  A Leader may become a Follower.
  First, we define the state transition *to* a candidate.
-}
convertToCandidate :: Server a -> Server a
convertToCandidate server@(Follower serverId followerState log') =
    let votesResponded' = HS.fromList [serverId]
        votesGranted' = HS.fromList [serverId]
        newState =
            CandidateState
                { currentTerm = incrementServerLogTerm server
                , commitIndex = followerState.commitIndex
                , lastApplied = followerState.lastApplied
                , allServerIds = followerState.allServerIds
                , votesResponded = votesResponded'
                , votesGranted = votesGranted'
                }
     in Candidate serverId newState log'
convertToCandidate server@(Candidate serverId candidateState log') =
    let
        votesResponded' = HS.fromList [serverId]
        votesGranted' = HS.fromList [serverId]
        newState = candidateState{
            currentTerm = incrementServerLogTerm server
            , votesResponded = votesResponded'
            , votesGranted = votesGranted'
            } :: CandidateState
     in Candidate serverId newState log'
convertToCandidate server = server -- Leaders cannot be converted into candidates

{- |  Here, we convert a server to a leader.
 Only a Candidate may be converted
-}
convertToLeader :: Server a -> Server a
convertToLeader (Candidate serverId candidateState log') =
    let serverIds = candidateState.allServerIds
        serverIdList = HS.toList serverIds
        newState =
            LeaderState
                { currentTerm = getCurrentTerm candidateState
                , commitIndex = candidateState.commitIndex
                , lastApplied = candidateState.lastApplied
                , allServerIds = candidateState.allServerIds
                , nextIndex = HM.fromList $ map (\s -> (s, RL.nextLogIndex log')) serverIdList
                , matchIndex = HM.fromList $ map (\s -> (s, RL.startLogIndex)) serverIdList
                }
     in Leader serverId newState log'
convertToLeader server = server

{- | After a RequestVoteResponse has been returned, we need to evaluate whether
 the Candidate has won, lost, or needs to keep waiting.
 If the election timeout happens again, the Candidate may
 call another election and convert itself to a *new* Candidate.
-}
maybePromoteCandidate :: Server a -> Server a
maybePromoteCandidate server@(Candidate _ candidateState _)
    | hasQuorum respondedVotes && hasQuorum grantedVotes = convertToLeader server
    | hasQuorum respondedVotes && (not . hasQuorum $ grantedVotes) = convertToFollower server
    | otherwise = server
  where
    quorum = calcQuorum (candidateState.allServerIds)
    hasQuorum n = n >= quorum
    respondedVotes = HS.size (votesResponded candidateState)
    grantedVotes = HS.size (votesGranted candidateState)
maybePromoteCandidate server = server

{- | Quorum is an important attribute for a Raft cluster.
 It is a simple majority of servers.
 Note: a cluster of 1 will be satisfied by a quorum of 1.
-}
calcQuorum :: HS.HashSet ServerId -> Int
calcQuorum serverIds = div (HS.size serverIds) 2 + 1

{- | Here we convert a server to a follower.
  Only a Candidate or a Leader may be converted.
  A Follower is returned unchanged.
-}
convertToFollower :: Server a -> Server a
convertToFollower (Candidate serverId candidateState log') =
    let newState =
            FollowerState
                { currentTerm = getCurrentTerm candidateState
                , commitIndex = candidateState.commitIndex
                , lastApplied = candidateState.lastApplied
                , allServerIds = candidateState.allServerIds
                , votedFor = Nothing
                , currentLeader = Nothing
                }
     in Follower serverId newState log'
convertToFollower (Leader serverId leaderState log') =
    let newState =
            FollowerState
                { currentTerm = getCurrentTerm leaderState
                , commitIndex = leaderState.commitIndex
                , lastApplied = leaderState.lastApplied
                , allServerIds = leaderState.allServerIds
                , votedFor = Nothing
                , currentLeader = Nothing
                }
     in Follower serverId newState log'
convertToFollower follower@Follower {} = follower

{- | Receipt of RPCs means some state change and possibly a server change.
  Here, we handle the append entries RPC which should be sent by the supposed leader
 to all other servers.
 Note: only a Follower will update its log in response to the AppendEntriesRPC.
-}
handleAppendEntries :: RM.AppendEntriesRPC a -> Server a -> (Maybe RM.AppendEntriesResponseRPC, Server a)
handleAppendEntries msg (Follower serverId state serverLog) =
    let msgTerm = RM.getTerm msg
        msgTermValid = msgTerm >= getCurrentTerm state
        (updatedLog, success') =
            if msgTermValid && appendReqCheckLogOk msg serverLog
                then RL.appendEntries (RM.prevLogIndex msg) (RM.prevLogTerm msg) (RM.logEntries msg) serverLog
                else (serverLog, False)
        -- Make sure to reset any previous votedFor status
        state' = appendEntriesUpdateState msg success' (RL.logLastIndex updatedLog) state
        server' = updateServerTerm msgTerm (Follower serverId state' updatedLog)
        response = Just $ mkAppendEntriesResponse success' msg server'
     in (response, server')
handleAppendEntries msg server@(Candidate _ state _)
    | RM.getTerm msg > getCurrentTerm state = handleAppendEntries msg (convertToFollower server)
    | otherwise = (Nothing, server)
handleAppendEntries msg server@(Leader _ state _)
    | RM.getTerm msg > getCurrentTerm state = handleAppendEntries msg (convertToFollower server)
    | otherwise = (Nothing, server)

-- | Some bookkeeping is required if the AppendEntriesRPC successfully updated the server's log
appendEntriesUpdateState :: RM.AppendEntriesRPC a -> Bool -> RL.LogIndex -> FollowerState -> FollowerState
appendEntriesUpdateState msg success' latestIndex state =
    state{votedFor = Nothing, currentLeader = leaderUpdate, commitIndex = latestCommitted}
  where
    leaderUpdate = if success' then Just $ RM.getSource msg else Nothing
    msgCommitIndex = msg.commitIndex
    currentCommitIndex = state.commitIndex
    latestCommitted = if success' && msgCommitIndex > currentCommitIndex then min msgCommitIndex latestIndex else currentCommitIndex

-- | Check append entries request has expected log.
-- Does this duplicate the one in Raft.Log?
-- This function should probably be collapsed with the one in Raft.Log
appendReqCheckLogOk :: RM.AppendEntriesRPC a -> RL.Log a -> Bool
appendReqCheckLogOk req log'
    | prevIndex == RL.startLogIndex = True
    | otherwise =
        prevIndex > RL.startLogIndex
            && prevIndex < RL.nextLogIndex log'
            && RM.prevLogTerm req == RL.logTermAtIndex prevIndex log'
  where
    prevIndex = RM.prevLogIndex req

-- | A Leader upon receiving an RM.AppendEntriesResponseRPC must update its state
handleAppendEntriesResponse :: RM.AppendEntriesResponseRPC -> Server a -> Server a
handleAppendEntriesResponse response server@(Leader serverId state serverLog)
    | RM.staleTerm response (getCurrentTerm state) = server  -- is this correct?
    | RM.success response =
        let
            msgSrc = RM.getSource response
            msgMatchIdx = RM.matchIndex response
            udpatedMatchIdx = HM.adjust (max msgMatchIdx) msgSrc (matchIndex state)
            updatedNextIdx = HM.insert msgSrc (RL.incrementLogIndex msgMatchIdx) (nextIndex state)
            state' = state {nextIndex=updatedNextIdx, matchIndex=udpatedMatchIdx}
        in
            checkLeaderAppliedCommitted (Leader serverId state' serverLog)
    | otherwise =
        let
            updatedNextIdx = HM.adjust RL.decrementLogIndex (RM.getSource response) (nextIndex state)
            state' = state {nextIndex=updatedNextIdx}
        in
            Leader serverId state' serverLog
handleAppendEntriesResponse _ server = server

checkLeaderAppliedCommitted :: Server a -> Server a
checkLeaderAppliedCommitted (Leader serverId state serverLog) =
    let
        allMatches = sort . HM.elems . matchIndex $ state
        matchMedian = div (length allMatches) 2
        numCommitted = if matchMedian >= 0 then allMatches !! matchMedian else RL.startLogIndex
        commitIdx = max numCommitted (state.commitIndex)
        -- TODO: Figure out what to do about lastApplied and "apply" entries
        state' = state{commitIndex=commitIdx, lastApplied=commitIdx} :: LeaderState
    in
        Leader serverId state' serverLog
checkLeaderAppliedCommitted server = server

-- | Leaders generate AppendEntriesRPCs.
generateAppendEntriesRPC :: Server a -> ServerId -> Maybe (RM.AppendEntriesRPC a)
generateAppendEntriesRPC server@(Leader serverId state _) recvr = do
    (entries, prevIdx, prevTerm) <- getLogEntriesForRecvr server recvr
    Just $
        RM.AppendEntriesRPC
            { RM.sourceAndDest = RM.SourceDest{RM.source = serverId, RM.dest = recvr}
            , RM.prevLogIndex = prevIdx
            , RM.prevLogTerm = prevTerm
            , RM.logEntries = entries
            , RM.commitIndex = state.commitIndex
            , RM.term = getCurrentTerm state
            }
generateAppendEntriesRPC _ _ = Nothing

getLogEntriesForRecvr :: Server a -> ServerId -> Maybe (RL.LogEntries a, RL.LogIndex, RL.LogTerm)
getLogEntriesForRecvr server@(Leader _ state log') recvr =
    let
        expectedIdx = HM.findWithDefault RL.startLogIndex recvr (nextIndex state)
        prevIdx = expectedIdx - 1
        prevTerm = RL.logTermAtIndex prevIdx log'
        entries = RL.slice expectedIdx (RL.logLastIndex log' - expectedIdx) log'
    in Just (entries, prevIdx, prevTerm)
getLogEntriesForRecvr _ _ = Nothing


generateAppendEntriesRPCList :: Server a -> [RM.AppendEntriesRPC a]
generateAppendEntriesRPCList server@(Leader serverId state _) =
    mapMaybe (generateAppendEntriesRPC server) . HS.toList $
        HS.difference (state.allServerIds) (HS.fromList [serverId])
generateAppendEntriesRPCList _ = []

{- | This function handles the request vote RPC. Followers can vote.
 If a Candidate finds out that it's behind, it may convert to a follower and then vote.
 The same may happen if it's a Leader.
-}
handleRequestVote :: RM.RequestVoteRPC -> Server a -> (Maybe RM.RequestVoteResponseRPC, Server a)
handleRequestVote request (Follower serverId state serverLog) =
    let msgTerm = RM.getTerm request
        msgTermValid = msgTerm >= getCurrentTerm state
        candidateIsUpToDate = msgTermValid && RL.logIsBehindOrEqual (request.lastLogTerm) (request.lastLogIndex) serverLog
        (voted, state') = if candidateIsUpToDate then voteForServer request state else (False, state)
        server' = updateServerTerm msgTerm (Follower serverId state' serverLog)
        response = mkRequestVoteResponse voted request server'
     in (Just response, server')
handleRequestVote request server@(Candidate _ state _)
    | RM.getTerm request > getCurrentTerm state = handleRequestVote request (convertToFollower server)
    | otherwise = (Just $ mkRequestVoteResponse False request server, server)
handleRequestVote request server@(Leader _ state _)
    | RM.getTerm request > getCurrentTerm state = handleRequestVote request (convertToFollower server)
    | otherwise = (Nothing, server)

-- | Check if voting possible and if so, modify `votedFor` on FollowerState
voteForServer :: RM.RequestVoteRPC -> FollowerState -> (Bool, FollowerState)
voteForServer request state
    | msgTerm >= serverTerm && hasntVotedYet = (True, state{votedFor = votesFor})
    | otherwise = (False, state)
  where
    msgTerm = RM.getTerm request
    serverTerm = getCurrentTerm state
    hasntVotedYet = isNothing . votedFor $ state
    votesFor = Just $ RM.getSource request

{- | A Candidate upon receiving a RM.RequestVoteResponseRPC must update its state
 If it wins an election, it should be converted to a leader.
-}
handleRequestVoteResponse :: RM.RequestVoteResponseRPC -> Server a -> Server a
handleRequestVoteResponse response server@(Candidate serverId state serverLog)
    | RM.staleTerm response (getCurrentTerm state) = server
    | otherwise =
        -- Q: Should the Candidate *check* that this respondent is a *member* of this cluster?
        let respondent = RM.getSource response
            state' = state{votesResponded = HS.insert respondent (votesResponded state)}
            state'' =
                if RM.voteGranted response
                    then state'{votesGranted = HS.insert respondent (votesGranted state')}
                    else state'
        in maybePromoteCandidate (Candidate serverId state'' serverLog)
handleRequestVoteResponse _ server = server

-- | Candidates generate RequestVoteRPCs for each server and send them in parallel.
generateRequestVoteRPC :: Server a -> ServerId -> Maybe RM.RequestVoteRPC
generateRequestVoteRPC (Candidate serverId state serverLog) recvr =
    Just $
        RM.RequestVoteRPC
            { RM.sourceAndDest = RM.SourceDest{RM.source = serverId, RM.dest = recvr}
            , RM.lastLogTerm = RL.logLastTerm serverLog
            , RM.lastLogIndex = RL.logLastIndex serverLog
            , RM.term = getCurrentTerm state
            }
generateRequestVoteRPC _ _ = Nothing

-- | We can generate all RequestVoteRPCs by mapping over the list of Server IDs (not including this one)
generateRequestVoteRPCList :: Server a -> [RM.RequestVoteRPC]
generateRequestVoteRPCList server@(Candidate serverId state _) =
    mapMaybe (generateRequestVoteRPC server) . HS.toList $
        HS.difference (state.allServerIds) (HS.fromList [serverId])
generateRequestVoteRPCList _ = []

{- | Any message received from a server with a term greater than this server's
 means this server must *immediately* update its term to the latest.
 This comes from Raft paper §5.1.
-}
updateServerTerm :: RL.LogTerm -> Server a -> Server a
updateServerTerm t (Follower serverId serverState log') = Follower serverId state' log'
  where
    state' =
        if t > getCurrentTerm serverState
            then serverState{currentTerm = t} :: FollowerState
            else serverState
updateServerTerm t (Candidate serverId serverState log') = Candidate serverId state' log'
  where
    state' =
        if t > getCurrentTerm serverState
            then serverState{currentTerm = t} :: CandidateState
            else serverState
updateServerTerm t (Leader serverId serverState log') = Leader serverId state' log'
  where
    state' =
        if t > getCurrentTerm serverState
            then serverState{currentTerm = t} :: LeaderState
            else serverState

-- | Candidates or Followers will need to *increment* their term when an election begins
incrementServerLogTerm :: Server a -> RL.LogTerm
incrementServerLogTerm (Follower _ state _) = RL.incrementLogTerm . getCurrentTerm $ state
incrementServerLogTerm (Candidate _ state _) = RL.incrementLogTerm . getCurrentTerm $ state
-- Leader's log term must not be incremented: they do not start elections.
incrementServerLogTerm (Leader _ state _) = getCurrentTerm state

-- | We pull this term out a lot, so this alias is useful
getCurrentTerm :: HasField "currentTerm" r RL.LogTerm => r -> RL.LogTerm
getCurrentTerm = getField @"currentTerm"

{- | Here servers will translate a request into a response
 | Only a follower should reply to an AppendEntriesRPC
-}
mkAppendEntriesResponse :: Bool -> RM.AppendEntriesRPC a -> Server a -> RM.AppendEntriesResponseRPC
mkAppendEntriesResponse successful request (Follower _ state serverLog) =
    RM.AppendEntriesResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (request.sourceAndDest)
        , matchIndex = RL.logLastIndex serverLog
        , success = successful
        , term = getCurrentTerm state
        }
mkAppendEntriesResponse _ request (Candidate _ state serverLog) =
    RM.AppendEntriesResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (request.sourceAndDest)
        , matchIndex = RL.logLastIndex serverLog
        , success = False
        , term = getCurrentTerm state
        }
mkAppendEntriesResponse _ request (Leader _ state serverLog) =
    RM.AppendEntriesResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (request.sourceAndDest)
        , matchIndex = RL.logLastIndex serverLog
        , success = False
        , term = getCurrentTerm state
        }

-- | Request vote responses may be handled by any server type
mkRequestVoteResponse :: Bool -> RM.RequestVoteRPC -> Server a -> RM.RequestVoteResponseRPC
mkRequestVoteResponse granted request (Follower _ state _) =
    RM.RequestVoteResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (request.sourceAndDest)
        , voteGranted = granted
        , term = state.currentTerm
        }
mkRequestVoteResponse granted request (Candidate _ state _) =
    RM.RequestVoteResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (request.sourceAndDest)
        , voteGranted = granted
        , term = state.currentTerm
        }
mkRequestVoteResponse granted request (Leader _ state _) =
    RM.RequestVoteResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (request.sourceAndDest)
        , voteGranted = granted
        , term = state.currentTerm
        }
