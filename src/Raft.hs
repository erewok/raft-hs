{-# LANGUAGE DataKinds                     #-}
{-# LANGUAGE DeriveGeneric                 #-}
{-# LANGUAGE DeriveFoldable                #-}
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

import Raft.Log
import Raft.Message
import Raft.Server



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

-- | Functions to generate RPCs:
-- Candidates generate RequestVoteRPCs.
-- Leaders generate AppendEntriesRPCs.
generateAppendEntriesRPC :: [ServerId] -> ServerId -> LeaderState -> Log a -> AppendEntriesRPC a
generateAppendEntriesRPC serverList leaderId state log' = undefined

generateRequestVoteRPC :: [ServerId] -> ServerId -> CandidateState -> Log a -> RequestVoteRPC
generateRequestVoteRPC serverList candidateId state log' = undefined

-- | Receipt of RPCs means some state change and possibly a server change.
--  Here, we handle the append entries RPC which will be sent by the leader
-- to all other servers.
handleAppendEntries :: AppendEntriesRPC a -> Server a -> (AppendEntriesResponseRPC, Server a)
handleAppendEntries msg server@(Follower serverId state log') =
    let
        msgTermValid = getField @"term" msg >= getField @"currentTerm" state
        success = msgTermValid && appendReqCheckLogOk msg log'
        (log'', success') = appendEntries (prevLogIndex msg) (prevLogTerm msg) (logEntries msg)  log'
        -- Make sure to reset any previous votedFor status
        state' = if success' then resetFollowerVotedFor state else state
        response = mkAppendEntriesResponse success msg state'
        server' = Follower serverId state' log''
    in
        (response, server')
handleAppendEntries msg server@(Candidate serverId state _) =
    let
        msgTermValid = getField @"term" msg == getField @"currentTerm" state
        server'@(Follower _ state' _) = convertToFollower server
        response = mkAppendEntriesResponse False msg state'
    in
        (response, server')
handleAppendEntries msg server@(Leader serverId state _) =
    let
        msgTermValid = getField @"term" msg >= getField @"currentTerm" state
    in
        undefined

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
            , term = getField @"currentTerm" state
        }
    in resp

mkRequestVoteResponse :: Bool -> RequestVoteRPC -> Server a -> RequestVoteResponseRPC
mkRequestVoteResponse granted request (Follower _ state _) =
    let
        senderRecvr = swapSourceAndDest (getField @"sourceAndDest" request)
        resp = RequestVoteResponseRPC {
            sourceAndDest = senderRecvr
            , voteGranted = granted
            , term = getField @"currentTerm" state
        }
    in resp
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

-- | Check append entries request has expected log
appendReqCheckLogOk :: AppendEntriesRPC a -> Log a -> Bool
appendReqCheckLogOk req log'
    | prevIndex == startLogIndex = True
    | otherwise =
         prevIndex > startLogIndex
            && prevIndex <= nextLogIndex log'
                && prevLogTerm req == logTermAtIndex prevIndex log'
    where prevIndex = prevLogIndex req


-- | Outbound communication with other nodes using channels
--  If the channel is full we _flush_ the channel and start adding values
--  The idea is that raft should be resilient enough that messages may be lost
--  and the server will merely try again later.


--  Fold over stream of incoming events and check that not too many ticks have
--  come through without a heartbeat received.
