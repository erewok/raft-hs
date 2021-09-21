{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SpecFixtures where

import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS
import Data.List (sort)
import Data.Maybe (fromJust)
import Data.Text (Text)
import qualified Data.Vector as V
import Test.QuickCheck (Arbitrary(..), Gen, chooseInt)

import Raft.Log
    ( startLogIndex,
      LogEntry(..),
      Log(..),
      LogIndex,
      LogTerm,
      incrementLogIndex,
      mkLogIndex,
      mkLogTerm )
import Raft.Message ( getTerm, SourceDest(..) )
import Raft.Server
import Raft.Shared ( ServerId(ServerId) )


-- | Server Generators
instance Arbitrary ServerId where
    arbitrary = ServerId <$> chooseInt (1, 20)


-- | Message Generators
instance Arbitrary SourceDest where
    arbitrary = do
        source <- ServerId <$> chooseInt (1, 4)
        dest <-   ServerId <$> chooseInt (5, 8)
        pure $ SourceDest{..}



-- | Log Generators
-- We will apply the following rule for these:
-- All *LogTerm*s in the list of entries are increasing
-- That is, entry n - 1 will have a term <= term for entry n

-- | Arbitrary instance of a Log that fulfills our required property of incrementing terms
instance (Eq a, Arbitrary a) => Arbitrary (Log a) where
    arbitrary = do
        entries <- arbitrary :: Gen (V.Vector (LogEntry a))
        let offset = if V.null entries then startLogIndex else (index . V.head $ entries) - 1
        pure $ Log {..}

instance (Eq a, Arbitrary a) => Arbitrary (V.Vector (LogEntry a)) where
    arbitrary = do
        start <- chooseInt (1, 5)
        entries' <- sort <$> arbitrary :: Gen [LogEntry a]
        pure $ V.imap (\idx val -> val { index = fromJust . mkLogIndex $ idx + start}) (V.fromList entries')

instance (Arbitrary a) => Arbitrary (LogEntry a) where
    arbitrary = do
        term <- arbitrary
        content <- arbitrary
        let index = incrementLogIndex startLogIndex
        pure $ LogEntry {..}

instance Arbitrary LogTerm where
    arbitrary = fromJust . mkLogTerm <$> chooseInt (1, 100)

instance Arbitrary LogIndex where
    arbitrary = fromJust . mkLogIndex <$> chooseInt (1, 20)

-- | We will mostly want to sample from the length of an existing log here
arbitraryLogIndex :: V.Vector (LogEntry a) -> Gen LogIndex
arbitraryLogIndex input = arbitraryLogIndex' (1, V.length input)

arbitraryLogIndex' :: (Int, Int) -> Gen LogIndex
arbitraryLogIndex' range = fromJust . mkLogIndex <$> chooseInt range

-- | The arbitraryLogIndex is functionally dependent on the size of the input log
-- So we will actually generate instances of this thing instead.
data ArbLogWithIndex a = ArbLogWithIndex { lgIndex :: LogIndex, lg :: Log a} deriving (Eq, Show)
instance (Eq a, Arbitrary a) => Arbitrary (ArbLogWithIndex a) where
    arbitrary = do
        lg <- arbitrary
        lgIndex <- arbitraryLogIndex (entries lg)
        pure $ ArbLogWithIndex {..}

-- | We need this instance to produce a vector where terms are increasing
instance (Eq a) => Ord (LogEntry a) where
    compare left right = compare (getTerm left) (getTerm right)

-- | Setup functions for building datasets
terms :: [LogTerm]
terms = map termMakerUnsafe [1..10]

termMakerUnsafe :: Int -> LogTerm
termMakerUnsafe = fromJust . mkLogTerm

indexes :: [LogIndex]
indexes = map indexMakerUnsafe [1..]

indexMakerUnsafe :: Int -> LogIndex
indexMakerUnsafe = fromJust . mkLogIndex

-- | Sample Logs are below
-- Log from Figure 6 in the paper
figure6Log :: Log Text
figure6Log = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "x <- 3", index = head indexes }
        , LogEntry {term = head terms, content = "y <- 1", index = indexes !! 1  }
        , LogEntry {term = head terms, content = "y <- 9", index = indexes !! 2  }
        , LogEntry {term = terms !! 1, content = "x <- 2", index = indexes !! 3  }
        , LogEntry {term = terms !! 2, content = "x <- 0", index = indexes !! 4  }
        , LogEntry {term = terms !! 2, content = "y <- 7", index = indexes !! 5  }
        , LogEntry {term = terms !! 2, content = "x <- 5", index = indexes !! 6  }
  ]}

-- | For figure7 the diverging *terms* matter more than content
-- so we just put the index in the string. We build these verbosely
-- because it's so easy to get lost and confused when just trying to *remember*
-- what they are and predict interactions with them.
figure7Leader :: Log Text
figure7Leader = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "1", index = head indexes }
        , LogEntry {term = head terms, content = "2", index = indexes !! 1 }
        , LogEntry {term = head terms, content = "3", index = indexes !! 2 }
        , LogEntry {term = terms !! 3, content = "4", index = indexes !! 3 }
        , LogEntry {term = terms !! 3, content = "5", index = indexes !! 4 }
        , LogEntry {term = terms !! 4, content = "6", index = indexes !! 5 }
        , LogEntry {term = terms !! 4, content = "7", index = indexes !! 6 }
        , LogEntry {term = terms !! 5, content = "8", index = indexes !! 7 }
        , LogEntry {term = terms !! 5, content = "9", index = indexes !! 8 }
        , LogEntry {term = terms !! 5, content = "10", index = indexes !! 9 }
  ]}

figure7A :: Log Text
figure7A = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "1", index = head indexes }
        , LogEntry {term = head terms, content = "2", index = indexes !! 1 }
        , LogEntry {term = head terms, content = "3", index = indexes !! 2 }
        , LogEntry {term = terms !! 3, content = "4", index = indexes !! 3 }
        , LogEntry {term = terms !! 3, content = "5", index = indexes !! 4 }
        , LogEntry {term = terms !! 4, content = "6", index = indexes !! 5 }
        , LogEntry {term = terms !! 4, content = "7", index = indexes !! 6 }
        , LogEntry {term = terms !! 5, content = "8", index = indexes !! 7 }
        , LogEntry {term = terms !! 5, content = "9", index = indexes !! 8 }
  ]}

figure7B :: Log Text
figure7B = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "1", index = head indexes }
        , LogEntry {term = head terms, content = "2", index = indexes !! 1 }
        , LogEntry {term = head terms, content = "3", index = indexes !! 2 }
        , LogEntry {term = terms !! 3, content = "4", index = indexes !! 3 }
  ]}

figure7C :: Log Text
figure7C = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "1", index = head indexes }
        , LogEntry {term = head terms, content = "2", index = indexes !! 1 }
        , LogEntry {term = head terms, content = "3", index = indexes !! 2 }
        , LogEntry {term = terms !! 3, content = "4", index = indexes !! 3 }
        , LogEntry {term = terms !! 3, content = "5", index = indexes !! 4 }
        , LogEntry {term = terms !! 4, content = "6", index = indexes !! 5 }
        , LogEntry {term = terms !! 4, content = "7", index = indexes !! 6 }
        , LogEntry {term = terms !! 5, content = "8", index = indexes !! 7 }
        , LogEntry {term = terms !! 5, content = "9", index = indexes !! 8 }
        , LogEntry {term = terms !! 5, content = "10", index = indexes !! 9 }
        , LogEntry {term = terms !! 5, content = "11", index = indexes !! 10 }
  ]}

figure7D :: Log Text
figure7D = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "1", index = head indexes }
        , LogEntry {term = head terms, content = "2", index = indexes !! 1 }
        , LogEntry {term = head terms, content = "3", index = indexes !! 2 }
        , LogEntry {term = terms !! 3, content = "4", index = indexes !! 3 }
        , LogEntry {term = terms !! 3, content = "5", index = indexes !! 4 }
        , LogEntry {term = terms !! 4, content = "6", index = indexes !! 5 }
        , LogEntry {term = terms !! 4, content = "7", index = indexes !! 6 }
        , LogEntry {term = terms !! 5, content = "8", index = indexes !! 7 }
        , LogEntry {term = terms !! 5, content = "9", index = indexes !! 8 }
        , LogEntry {term = terms !! 5, content = "10", index = indexes !! 9 }
        , LogEntry {term = terms !! 6, content = "11", index = indexes !! 10 }
        , LogEntry {term = terms !! 6, content = "12", index = indexes !! 11 }
        ]
    }

figure7E :: Log Text
figure7E = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "1", index = head indexes }
        , LogEntry {term = head terms, content = "2", index = indexes !! 1 }
        , LogEntry {term = head terms, content = "3", index = indexes !! 2 }
        , LogEntry {term = terms !! 3, content = "4", index = indexes !! 3 }
        , LogEntry {term = terms !! 3, content = "5", index = indexes !! 4 }
        , LogEntry {term = terms !! 3, content = "6", index = indexes !! 5 }
        , LogEntry {term = terms !! 3, content = "7", index = indexes !! 6 }
  ]}

figure7F :: Log Text
figure7F = Log {
    offset = startLogIndex
    , entries = V.fromList [
        LogEntry {term = head terms, content = "1", index = head indexes }
        , LogEntry {term = head terms, content = "2", index = indexes !! 1 }
        , LogEntry {term = head terms, content = "3", index = indexes !! 2 }
        , LogEntry {term = terms !! 1, content = "4", index = indexes !! 3 }
        , LogEntry {term = terms !! 1, content = "5", index = indexes !! 4 }
        , LogEntry {term = terms !! 1, content = "6", index = indexes !! 5 }
        , LogEntry {term = terms !! 2, content = "7", index = indexes !! 6 }
        , LogEntry {term = terms !! 2, content = "8", index = indexes !! 7 }
        , LogEntry {term = terms !! 2, content = "9", index = indexes !! 8 }
        , LogEntry {term = terms !! 2, content = "10", index = indexes !! 9 }
        , LogEntry {term = terms !! 2, content = "11", index = indexes !! 10 }
  ]}


allServers :: HS.HashSet ServerId
allServers = HS.fromList [
    ServerId 1,
    ServerId 2,
    ServerId 3,
    ServerId 4,
    ServerId 5,
    ServerId 6,
    ServerId 7
    ]

fig7ServerLeader :: Server Text
fig7ServerLeader = Leader (ServerId 1) startState figure7Leader
    where startState = LeaderState {
        currentTerm = termMakerUnsafe 6,
        commitIndex = indexMakerUnsafe 9,
        lastApplied = indexMakerUnsafe 5,
        allServerIds = allServers,
        matchIndex = HM.fromList [
            (ServerId 2, indexMakerUnsafe 9),
            (ServerId 3, indexMakerUnsafe 4),
            (ServerId 4, indexMakerUnsafe 10),
            (ServerId 5, indexMakerUnsafe 7),
            (ServerId 6, indexMakerUnsafe 10),
            (ServerId 7, indexMakerUnsafe 10)
        ],
        nextIndex = HM.fromList [
            (ServerId 2, indexMakerUnsafe 10),
            (ServerId 3, indexMakerUnsafe 5),
            (ServerId 4, indexMakerUnsafe 11),
            (ServerId 5, indexMakerUnsafe 8),
            (ServerId 6, indexMakerUnsafe 11),
            (ServerId 7, indexMakerUnsafe 11)
        ]
    }

fig7ServerFollowerA :: Server Text
fig7ServerFollowerA = Follower (ServerId 2) startState figure7A
    where startState = FollowerState {
        currentTerm = termMakerUnsafe 5,
        commitIndex = indexMakerUnsafe 9,
        lastApplied = indexMakerUnsafe 5,
        allServerIds = allServers,
        votedFor  = Nothing,
        currentLeader = Nothing
    }