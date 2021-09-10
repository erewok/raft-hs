{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}

module SpecFixtures where

import Data.List (sort)
import Data.Maybe (fromJust)
import Data.Text (Text)
import qualified Data.Vector as V
import Test.QuickCheck (Arbitrary(..), Gen, chooseInt)

import Raft.Log

-- | Log Generators
-- We will apply the following rule for these:
-- All *LogTerm*s in the list of entries are increasing
-- That is, entry n - 1 will have a term <= the term for entry n

-- | Arbitrary instance of a Log that fulfills our required property of incrementing terms
instance (Eq a, Arbitrary a) => Arbitrary (Log a) where
    arbitrary = do
        entries <- arbitrary :: Gen (V.Vector (LogEntry a))
        pure $ Log {..}

instance (Eq a, Arbitrary a) => Arbitrary (V.Vector (LogEntry a)) where
    arbitrary = do
        entries <- sort <$> arbitrary :: Gen [LogEntry a]
        pure $ V.fromList entries

instance (Arbitrary a) => Arbitrary (LogEntry a) where
    arbitrary = do
        term <- arbitrary
        content <- arbitrary
        pure $ LogEntry {..}

instance Arbitrary LogTerm where
    arbitrary = (fromJust . mkLogTerm) <$> chooseInt (0, 100)

-- | We will mostly want to sample from the length of an existing log here
arbitraryLogIndex :: V.Vector (LogEntry a) -> Gen LogIndex
arbitraryLogIndex input
    | V.null input = arbitraryLogIndex' (0, 0)
    | otherwise    = arbitraryLogIndex' (0, V.length input - 1)

arbitraryLogIndex' :: (Int, Int) -> Gen LogIndex
arbitraryLogIndex' range = fromJust . mkLogIndex <$> chooseInt range

-- | The arbitraryLogIndex is functionally dependent on the size of the input log
-- So we will actually generate instances of this thing instead
data ArbLogWithIndex a = ArbLogWithIndex { lgIndex :: LogIndex, lg :: Log a} deriving (Eq, Show)
instance (Eq a, Arbitrary a) => Arbitrary (ArbLogWithIndex a) where
    arbitrary = do
        lg <- arbitrary
        lgIndex <- arbitraryLogIndex (entries lg)
        pure $ ArbLogWithIndex {..}

-- | We need this instance to produce a vector where terms are increasing
instance (Eq a) => Ord (LogEntry a) where
    compare left right = compare (term left) (term right)

-- | Setup functions for building datasets
terms :: [LogTerm]
terms = map termMakerUnsafe [1..10]

termMakerUnsafe :: Int -> LogTerm
termMakerUnsafe n = fromJust . mkLogTerm $ n

-- | Log from Figure 6 in the paper
figure6Log :: Log Text
figure6Log = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "x <- 3" }
  , LogEntry {term = terms !! 0, content = "y <- 1" }
  , LogEntry {term = terms !! 0, content = "y <- 9" }
  , LogEntry {term = terms !! 1, content = "x <- 2" }
  , LogEntry {term = terms !! 2, content = "x <- 0" }
  , LogEntry {term = terms !! 2, content = "y <- 7" }
  , LogEntry {term = terms !! 2, content = "x <- 5" }
  , LogEntry {term = terms !! 2, content = "x <- 4" }
  ]}

-- | For figure7 the diverging *terms* matter more than content
-- so we just put the index in the string. We build these verbosely
-- because it's so easy to get lost and confused when just trying to *remember*
-- what they are and predict interactions with them.
figure7Leader :: Log Text
figure7Leader = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "1" }
  , LogEntry {term = terms !! 0, content = "2" }
  , LogEntry {term = terms !! 0, content = "3" }
  , LogEntry {term = terms !! 3, content = "4" }
  , LogEntry {term = terms !! 3, content = "5" }
  , LogEntry {term = terms !! 4, content = "6" }
  , LogEntry {term = terms !! 4, content = "7" }
  , LogEntry {term = terms !! 5, content = "8" }
  , LogEntry {term = terms !! 5, content = "9" }
  , LogEntry {term = terms !! 5, content = "10" }
  ]}

figure7A :: Log Text
figure7A = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "1" }
  , LogEntry {term = terms !! 0, content = "2" }
  , LogEntry {term = terms !! 0, content = "3" }
  , LogEntry {term = terms !! 3, content = "4" }
  , LogEntry {term = terms !! 3, content = "5" }
  , LogEntry {term = terms !! 4, content = "6" }
  , LogEntry {term = terms !! 4, content = "7" }
  , LogEntry {term = terms !! 5, content = "8" }
  , LogEntry {term = terms !! 5, content = "9" }
  ]}

figure7B :: Log Text
figure7B = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "1" }
  , LogEntry {term = terms !! 0, content = "2" }
  , LogEntry {term = terms !! 0, content = "3" }
  , LogEntry {term = terms !! 3, content = "4" }
  ]}

figure7C :: Log Text
figure7C = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "1" }
  , LogEntry {term = terms !! 0, content = "2" }
  , LogEntry {term = terms !! 0, content = "3" }
  , LogEntry {term = terms !! 3, content = "4" }
  , LogEntry {term = terms !! 3, content = "5" }
  , LogEntry {term = terms !! 4, content = "6" }
  , LogEntry {term = terms !! 4, content = "7" }
  , LogEntry {term = terms !! 5, content = "8" }
  , LogEntry {term = terms !! 5, content = "9" }
  , LogEntry {term = terms !! 5, content = "10" }
  , LogEntry {term = terms !! 5, content = "11" }
  ]}

figure7D :: Log Text
figure7D = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "1" }
  , LogEntry {term = terms !! 0, content = "2" }
  , LogEntry {term = terms !! 0, content = "3" }
  , LogEntry {term = terms !! 3, content = "4" }
  , LogEntry {term = terms !! 3, content = "5" }
  , LogEntry {term = terms !! 4, content = "6" }
  , LogEntry {term = terms !! 4, content = "7" }
  , LogEntry {term = terms !! 5, content = "8" }
  , LogEntry {term = terms !! 5, content = "9" }
  , LogEntry {term = terms !! 5, content = "10" }
  , LogEntry {term = terms !! 6, content = "11" }
  , LogEntry {term = terms !! 6, content = "12" }
  ]}

figure7E :: Log Text
figure7E = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "1" }
  , LogEntry {term = terms !! 0, content = "2" }
  , LogEntry {term = terms !! 0, content = "3" }
  , LogEntry {term = terms !! 3, content = "4" }
  , LogEntry {term = terms !! 3, content = "5" }
  , LogEntry {term = terms !! 3, content = "6" }
  , LogEntry {term = terms !! 3, content = "7" }
  ]}

figure7F :: Log Text
figure7F = Log { entries = V.fromList [
  LogEntry {term = terms !! 0, content = "1" }
  , LogEntry {term = terms !! 0, content = "2" }
  , LogEntry {term = terms !! 0, content = "3" }
  , LogEntry {term = terms !! 1, content = "4" }
  , LogEntry {term = terms !! 1, content = "5" }
  , LogEntry {term = terms !! 1, content = "6" }
  , LogEntry {term = terms !! 2, content = "7" }
  , LogEntry {term = terms !! 2, content = "8" }
  , LogEntry {term = terms !! 2, content = "9" }
  , LogEntry {term = terms !! 2, content = "10" }
  , LogEntry {term = terms !! 2, content = "11" }
  ]}