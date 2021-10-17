{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedLabels #-}
{-# LANGUAGE TypeApplications #-}

module Raft.ServerSpec where

import qualified Data.HashSet as HS
import GHC.Records (HasField (..))
import Test.Hspec
import SpecFixtures

import qualified Raft.Log as RL
import qualified Raft.Message as RM
import Raft.Server
import Raft.Shared ( ServerId(ServerId) )


checkCandidate :: Spec
checkCandidate = do
    describe "converToCandidate variations" $ do
        it "Can convert Follower with incremented term, responded, and granted" $ do
            let (Candidate serverId newState newLog) = convertToCandidate fig7ServerFollowerA
                newTerm = getField @"currentTerm" newState
                incrementedTerm = incrementServerLogTerm fig7ServerFollowerA
            newTerm `shouldBe` incrementedTerm
            votesResponded newState `shouldBe` HS.fromList [serverId]
            votesGranted newState `shouldBe` HS.fromList [serverId]
        it "Can convert Candidate with incremented term, responded, and granted" $ do
            let newCandidate = convertToCandidate fig7ServerFollowerA
                (Candidate serverId newState newLog) = convertToCandidate newCandidate
                newTerm = getField @"currentTerm" newState
                incrementedTerm = RL.incrementLogTerm . incrementServerLogTerm $ fig7ServerFollowerA
            newTerm `shouldBe` incrementedTerm
            votesResponded newState `shouldBe` HS.fromList [serverId]
            votesGranted newState `shouldBe` HS.fromList [serverId]
        it "Cannot convert a leader" $ do
            let (Leader expectedId expectedState _) = fig7ServerLeader
                (Leader newId newState _) = convertToCandidate fig7ServerLeader
            (newId, newState) `shouldBe` (expectedId, expectedState)

    describe "generateRequestVoteRPC" $ do
        it "Candidate can generate a list of RequestVoteRPC" $ do
            let newCand@(Candidate serverId' state' log'') = convertToCandidate fig7ServerFollowerA
                voteResp = RM.RequestVoteResponseRPC (RM.SourceDest {RM.dest = ServerId 2, RM.source = ServerId 3} ) True (getCurrentTerm state)
                newCandWithVote@(Candidate serverId state log') = handleRequestVoteResponse voteResp newCand
                requestVotes = generateRequestVoteRPCList newCand
                expectedLen = HS.size allServers - 1
            length requestVotes `shouldBe` expectedLen
            -- Even though a vote had been received, we should see a reset here
            HS.fromList (map RM.getSource requestVotes) `shouldBe` HS.fromList [serverId]
            HS.fromList (map RM.getDest requestVotes) `shouldBe` HS.difference allServers (HS.fromList [serverId])
            map RM.lastLogTerm requestVotes `shouldBe` replicate expectedLen (RL.logLastTerm log')
            map RM.lastLogIndex requestVotes `shouldBe` replicate expectedLen (RL.logLastIndex log')
            map (getField @"term") requestVotes `shouldBe` replicate expectedLen (getCurrentTerm state)
        it "A Leader will not generate RequestVoteRPC" $ do
            generateRequestVoteRPCList fig7ServerLeader `shouldBe` []
        it "A Follower will not generate RequestVoteRPC" $ do
            generateRequestVoteRPCList fig7ServerFollowerA `shouldBe` []

    -- describe "handleRequestVoteResponse" $ do
    --     it "Candidate converts to leader if it has a quorum" $ do


-- generateRequestVoteRPCTest :: Spec
-- generateRequestVoteRPCTest = do
--     describe "Variations from figure 7" $ do
--         it "Generate append entries for followers missing" $ do
--             undefined


-- generateAppendEntriesTest :: Spec
-- generateAppendEntriesTest = do
--     describe "Variations from figure 7" $ do
--         it "Generate append entries for followers missing" $ do
--             undefined

checkFollower :: Spec
checkFollower = do
    describe "convertToFollower variations" $ do
        it "Can convert to Follower from Candidate" $ do
            let candidate@(Candidate _ cstate _) = convertToCandidate fig7ServerFollowerA
                follower@(Follower _ fstate _ ) = convertToFollower candidate
            getCurrentTerm fstate `shouldBe` getCurrentTerm cstate
            votedFor fstate `shouldBe` Nothing
            currentLeader fstate `shouldBe` Nothing
        it "Can convert to Follower from Leader" $ do
            let follower@(Follower _ fstate _ ) = convertToFollower fig7ServerLeader
                (Leader _ lstate _ ) = fig7ServerLeader
            getCurrentTerm fstate `shouldBe` getCurrentTerm lstate
            votedFor fstate `shouldBe` Nothing
            currentLeader fstate `shouldBe` Nothing
        it "A Follower should remain Follower" $ do
            convertToFollower fig7ServerFollowerA `shouldBe` fig7ServerFollowerA


serverSpec :: Spec
serverSpec = do
    checkCandidate
    checkFollower
