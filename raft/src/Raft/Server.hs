{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedLabels #-}
{-# Language PolyKinds #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# Language StandaloneKindSignatures #-}
{-# LANGUAGE TypeApplications #-}

module Raft.Server where

import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import Data.Maybe (isNothing, mapMaybe)
import GHC.Records (HasField (..))

import qualified Raft.Log as RL
import qualified Raft.Message as RM
import Raft.Shared (ServerId)

-- | A raft server may only be one of three different types.
-- data Server a =
--     Follower ServerId FollowerState (RL.Log a)
--     | Candidate ServerId CandidateState (RL.Log a)
--     | Leader ServerId LeaderState (RL.Log a)

data ServerType = Follower | Candidate | Leader
data Server (s :: ServerType) a where
    Fol :: ServerId -> FollowerState -> RL.Log a -> Server 'Follower a
    Cand :: ServerId -> CandidateState -> RL.Log a -> Server 'Candidate a
    Lead :: ServerId -> LeaderState -> RL.Log a -> Server 'Leader a

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
convertFollowerToCandidate :: Server 'Follower a -> Server 'Candidate a
convertFollowerToCandidate server@(Fol serverId followerState log') =
    let votesResponded' = HS.fromList [serverId]
        votesGranted' = HS.fromList [serverId]
        newState =
            CandidateState
                { currentTerm = incrementServerLogTerm server
                , commitIndex = getField @"commitIndex" followerState
                , lastApplied = getField @"lastApplied" followerState
                , allServerIds = getField @"allServerIds" followerState
                , votesResponded = votesResponded'
                , votesGranted = votesGranted'
                }
     in Cand serverId newState log'
convertCandidateToCandidate :: Server 'Candidate a -> Server 'Candidate a
convertCandidateToCandidate server@(Cand serverId candidateState log') =
    let
        votesResponded' = HS.fromList [serverId]
        votesGranted' = HS.fromList [serverId]
        newState = candidateState{
            currentTerm = incrementServerLogTerm server
            , votesResponded = votesResponded'
            , votesGranted = votesGranted'
            } :: CandidateState
     in Cand serverId newState log'
-- convertToCandidate server = server -- Leaders cannot be converted into candidates

{- |  Here, we convert a server to a leader.
 Only a Candidate may be converted
-}
convertToLeader :: Server 'Candidate a -> Server 'Leader a
convertToLeader (Cand serverId candidateState log') =
    let serverIds = getField @"allServerIds" candidateState
        serverIdList = HS.toList serverIds
        newState =
            LeaderState
                { currentTerm = getStateTerm candidateState
                , commitIndex = getField @"commitIndex" candidateState
                , lastApplied = getField @"lastApplied" candidateState
                , allServerIds = serverIds
                , nextIndex = HM.fromList $ map (\s -> (s, RL.nextLogIndex log')) serverIdList
                , matchIndex = HM.fromList $ map (\s -> (s, RL.startLogIndex)) serverIdList
                }
     in Lead serverId newState log'
-- convertToLeader server = server

{- | After a RequestVoteResponse has been returned, we need to evaluate whether
 the Candidate has won, lost, or needs to keep waiting.
 If the election timeout happens again, the Candidate may
 call another election and convert itself to a *new* Candidate.
-}
maybePromoteCandidate :: Server 'Candidate a -> Server t a
maybePromoteCandidate server@(Cand _ candidateState _)
    | hasQuorum respondedVotes && hasQuorum grantedVotes = convertToLeader server
    | hasQuorum respondedVotes && (not . hasQuorum $ grantedVotes) = convertToFollower server
    | otherwise = server
  where
    quorum = calcQuorum (getField @"allServerIds" candidateState)
    hasQuorum n = n >= quorum
    respondedVotes = HS.size (votesResponded candidateState)
    grantedVotes = HS.size (votesGranted candidateState)
-- maybePromoteCandidate server = server

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
convertToFollower :: Server t a -> Server 'Follower a
convertToFollower (Cand serverId candidateState log') =
    let newState =
            FollowerState
                { currentTerm = getStateTerm candidateState
                , commitIndex = getField @"commitIndex" candidateState
                , lastApplied = getField @"lastApplied" candidateState
                , allServerIds = getField @"allServerIds" candidateState
                , votedFor = Nothing
                , currentLeader = Nothing
                }
     in Fol serverId newState log'
convertToFollower (Lead serverId leaderState log') =
    let newState =
            FollowerState
                { currentTerm = getStateTerm leaderState
                , commitIndex = getField @"commitIndex" leaderState
                , lastApplied = getField @"lastApplied" leaderState
                , allServerIds = getField @"allServerIds" leaderState
                , votedFor = Nothing
                , currentLeader = Nothing
                }
     in Fol serverId newState log'
convertToFollower follower@Fol {} = follower

{- | Receipt of RPCs means some state change and possibly a server change.
  Here, we handle the append entries RPC which should be sent by the supposed leader
 to all other servers.
 Note: only a Follower will update its log in response to the AppendEntriesRPC.
-}
handleAppendEntries :: RM.AppendEntriesRPC a -> Server t a -> (Maybe RM.AppendEntriesResponseRPC, Server u a)
handleAppendEntries msg (Fol serverId state serverLog) =
    let msgTerm = getField @"term" msg
        msgTermValid = msgTerm >= getStateTerm state
        (updatedLog, success') =
            if msgTermValid && appendReqCheckLogOk msg serverLog
                then RL.appendEntries (RM.prevLogIndex msg) (RM.prevLogTerm msg) (RM.logEntries msg) serverLog
                else (serverLog, False)
        -- Make sure to reset any previous votedFor status
        state' = appendEntriesUpdateState msg success' (RL.logLastIndex updatedLog) state
        server' = updateServerTerm msgTerm (Fol serverId state' updatedLog)
        response = Just $ mkAppendEntriesResponse success' msg server'
     in (response, server')
handleAppendEntries msg server@(Cand _ state _)
    | getField @"term" msg > getStateTerm state = handleAppendEntries msg (convertToFollower server)
    | otherwise = (Nothing, server)
handleAppendEntries msg server@(Lead _ state _)
    | getField @"term" msg > getStateTerm state = handleAppendEntries msg (convertToFollower server)
    | otherwise = (Nothing, server)

-- | Some bookkeeping is required if the AppendEntriesRPC successfully updated the server's log
appendEntriesUpdateState :: RM.AppendEntriesRPC a -> Bool -> RL.LogIndex -> FollowerState -> FollowerState
appendEntriesUpdateState msg success' latestIndex state =
    state{votedFor = Nothing, currentLeader = leaderUpdate, commitIndex = latestCommitted}
  where
    leaderUpdate = if success' then Just $ RM.getSource msg else Nothing
    msgCommitIndex = getField @"commitIndex" msg
    currentCommitIndex = getField @"commitIndex" state
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
handleAppendEntriesResponse :: RM.AppendEntriesResponseRPC -> Server t a -> Server t a
handleAppendEntriesResponse response server@(Lead serverId state serverLog) = undefined
handleAppendEntriesResponse _ server = server

-- | Leaders generate AppendEntriesRPCs.
generateAppendEntriesRPC :: ServerId -> Server t a -> Maybe (RM.AppendEntriesRPC a)
generateAppendEntriesRPC recvr server = Nothing

generateAppendEntriesRPCList :: Server t a -> [RM.AppendEntriesRPC a]
generateAppendEntriesRPCList server@(Lead serverId state log') = undefined -- don't send to self!
generateAppendEntriesRPCList server = []

{- | This function handles the request vote RPC. Followers can vote.
 If a Candidate finds out that it's behind, it may convert to a follower and then vote.
 The same may happen if it's a Leader.
-}
handleRequestVote :: RM.RequestVoteRPC -> Server t a -> (Maybe RM.RequestVoteResponseRPC, Server u a)
handleRequestVote request (Fol serverId state serverLog) =
    let msgTerm = RM.getTerm request
        msgTermValid = msgTerm >= getStateTerm state
        candidateIsUpToDate = msgTermValid && RL.logIsBehindOrEqual (getField @"lastLogTerm" request) (getField @"lastLogIndex" request) serverLog
        (voted, state') = if candidateIsUpToDate then voteForServer request state else (False, state)
        server' = updateServerTerm msgTerm (Fol serverId state' serverLog)
        response = mkRequestVoteResponse voted request server'
     in (Just response, server')
handleRequestVote request server@(Cand _ state _)
    | RM.getTerm request > getStateTerm state = handleRequestVote request (convertToFollower server)
    | otherwise = (Just $ mkRequestVoteResponse False request server, server)
handleRequestVote request server@(Lead _ state _)
    | RM.getTerm request > getStateTerm state = handleRequestVote request (convertToFollower server)
    | otherwise = (Nothing, server)

-- | Check if voting possible and if so, modify `votedFor` on FollowerState
voteForServer :: RM.RequestVoteRPC -> FollowerState -> (Bool, FollowerState)
voteForServer request state
    | msgTerm >= serverTerm && hasntVotedYet = (True, state{votedFor = votesFor})
    | otherwise = (False, state)
  where
    msgTerm = RM.getTerm request
    serverTerm = getStateTerm state
    hasntVotedYet = isNothing . votedFor $ state
    votesFor = Just $ RM.getSource request

{- | A Candidate upon receiving a RM.RequestVoteResponseRPC must update its state
 If it wins an election, it should be converted to a leader.
-}
handleRequestVoteResponse :: RM.RequestVoteResponseRPC -> Server 'Candidate a -> Server t a
handleRequestVoteResponse response (Cand serverId state serverLog) =
    -- Q: Should the Candidate *check* that this respondent is a *member* of this cluster?
    let respondent = RM.getSource response
        state' = state{votesResponded = HS.insert respondent (votesResponded state)}
        state'' =
            if RM.voteGranted response
                then state'{votesGranted = HS.insert respondent (votesGranted state')}
                else state'
     in maybePromoteCandidate (Cand serverId state'' serverLog)
-- handleRequestVoteResponse _ server = server

-- | Candidates generate RequestVoteRPCs for each server and send them in parallel.
generateRequestVoteRPC :: Server 'Candidate a -> ServerId -> Maybe RM.RequestVoteRPC
generateRequestVoteRPC (Cand serverId state serverLog) recvr =
    Just $
        RM.RequestVoteRPC
            { RM.sourceAndDest = RM.SourceDest{RM.source = serverId, RM.dest = recvr}
            , RM.lastLogTerm = RL.logLastTerm serverLog
            , RM.lastLogIndex = RL.logLastIndex serverLog
            , RM.term = getStateTerm state
            }
-- generateRequestVoteRPC _ _ = Nothing

-- | We can generate all RequestVoteRPCs by mapping over the list of Server IDs (not including this one)
generateRequestVoteRPCList :: Server 'Candidate a -> [RM.RequestVoteRPC]
generateRequestVoteRPCList server@(Cand serverId state _) =
    mapMaybe (generateRequestVoteRPC server) . HS.toList $
        HS.difference (getField @"allServerIds" state) (HS.fromList [serverId])
-- generateRequestVoteRPCList _ = []

{- | Any message received from a server with a term greater than this server's
 means this server must *immediately* update its term to the latest.
 This comes from Raft paper §5.1.
-}
updateServerTerm :: RL.LogTerm -> Server t a -> Server t a
updateServerTerm t (Fol serverId serverState log') = Fol serverId state' log'
  where
    state' =
        if t > getStateTerm serverState
            then serverState{currentTerm = t} :: FollowerState
            else serverState
updateServerTerm t (Cand serverId serverState log') = Cand serverId state' log'
  where
    state' =
        if t > getStateTerm serverState
            then serverState{currentTerm = t} :: CandidateState
            else serverState
updateServerTerm t (Lead serverId serverState log') = Lead serverId state' log'
  where
    state' =
        if t > getStateTerm serverState
            then serverState{currentTerm = t} :: LeaderState
            else serverState

-- | Candidates or Followers will need to *increment* their term when an election begins
incrementServerLogTerm :: Server t a -> RL.LogTerm
incrementServerLogTerm (Fol _ state _) = RL.incrementLogTerm . getStateTerm $ state
incrementServerLogTerm (Cand _ state _) = RL.incrementLogTerm . getStateTerm $ state
-- Leader's log term must not be incremented: they do not start elections.
incrementServerLogTerm (Lead _ state _) = getStateTerm state

-- | We pull this term out a lot, so this alias is useful
getStateTerm :: HasField "currentTerm" r RL.LogTerm => r -> RL.LogTerm
getStateTerm = getField @"currentTerm"

{- | Here servers will translate a request into a response
 | Only a follower should reply to an AppendEntriesRPC
-}
mkAppendEntriesResponse :: Bool -> RM.AppendEntriesRPC a -> Server t a -> RM.AppendEntriesResponseRPC
mkAppendEntriesResponse successful request (Fol _ state serverLog) =
    RM.AppendEntriesResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (getField @"sourceAndDest" request)
        , matchIndex = RL.logLastIndex serverLog
        , success = successful
        , term = getStateTerm state
        }
mkAppendEntriesResponse _ request (Cand _ state serverLog) =
    RM.AppendEntriesResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (getField @"sourceAndDest" request)
        , matchIndex = RL.logLastIndex serverLog
        , success = False
        , term = getStateTerm state
        }
mkAppendEntriesResponse _ request (Lead _ state serverLog) =
    RM.AppendEntriesResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (getField @"sourceAndDest" request)
        , matchIndex = RL.logLastIndex serverLog
        , success = False
        , term = getStateTerm state
        }

-- | Request vote responses may be handled by any server type
mkRequestVoteResponse :: Bool -> RM.RequestVoteRPC -> Server t a -> RM.RequestVoteResponseRPC
mkRequestVoteResponse granted request (Fol _ state _) =
    RM.RequestVoteResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (getField @"sourceAndDest" request)
        , voteGranted = granted
        , term = getField @"currentTerm" state
        }
mkRequestVoteResponse granted request (Cand _ state _) =
    RM.RequestVoteResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (getField @"sourceAndDest" request)
        , voteGranted = granted
        , term = getField @"currentTerm" state
        }
mkRequestVoteResponse granted request (Lead _ state _) =
    RM.RequestVoteResponseRPC
        { sourceAndDest = RM.swapSourceAndDest (getField @"sourceAndDest" request)
        , voteGranted = granted
        , term = getField @"currentTerm" state
        }
