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


checkCandidate :: Spec
checkCandidate = do
    describe "converToCandidate variations" $ do
        it "Can convert Follower with incremented term, responded, and granted" $ do
            let (Candidate serverId newState newLog) = convertToCandidate fig7ServerFollowerA
                newIndex = getField @"currentTerm" newState
                incrementedTerm = incrementServerLogTerm fig7ServerFollowerA
            newIndex `shouldBe` incrementedTerm
            votesResponded newState `shouldBe` HS.fromList [serverId]
            votesGranted newState `shouldBe` HS.fromList [serverId]
        it "Can convert Candidate with incremented term, responded, and granted" $ do
            let (Candidate serverId newState newLog) = convertToCandidate . convertToCandidate $ fig7ServerFollowerA
                newIndex = getField @"currentTerm" newState
                incrementedTerm = RL.incrementLogTerm . incrementServerLogTerm $ fig7ServerFollowerA
            newIndex `shouldBe` incrementedTerm
            votesResponded newState `shouldBe` HS.fromList [serverId]
            votesGranted newState `shouldBe` HS.fromList [serverId]
        it "Cannot convert a leader" $ do
            let (Leader expectedId expectedState _) = fig7ServerLeader
                (Leader newId newState _) = convertToCandidate fig7ServerLeader
            (newId, newState) `shouldBe` (expectedId, expectedState)

    describe "generateRequestVoteRPC" $ do
        it "Candidate can generate a list of RequestVoteRPC" $ do
            let newCand@(Candidate serverId state log') = convertToCandidate fig7ServerFollowerA
                requestVotes = generateRequestVoteRPCList newCand
                expectedLen = HS.size allServers - 1
            length requestVotes `shouldBe` expectedLen
            HS.fromList (map RM.getSource requestVotes) `shouldBe` HS.fromList [serverId]
            HS.fromList (map RM.getDest requestVotes) `shouldBe` HS.difference allServers (HS.fromList [serverId])
            map RM.lastLogTerm requestVotes `shouldBe` replicate expectedLen (RL.logLastTerm log')
            map RM.lastLogIndex requestVotes `shouldBe` replicate expectedLen (RL.logLastIndex log')
            map (getField @"term") requestVotes `shouldBe` replicate expectedLen (getStateTerm state)
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


serverSpec :: Spec
serverSpec = do
    checkCandidate
