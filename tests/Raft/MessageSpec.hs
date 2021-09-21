module Raft.MessageSpec where

import Test.Hspec
import Test.Hspec.QuickCheck (prop)
import Test.QuickCheck (Property, counterexample, property, (.||.), (===))

import Raft.Message

import SpecFixtures

prop_isomorphicSourceAndDest :: SourceDest -> Property
prop_isomorphicSourceAndDest sd = (swapSourceAndDest . swapSourceAndDest $ sd) === sd

prop_checkSource :: SourceDest -> Property
prop_checkSource sd = (source . swapSourceAndDest $ sd) === (dest sd)

prop_checkDest :: SourceDest -> Property
prop_checkDest sd = (dest . swapSourceAndDest $ sd) === (source sd)

msgSpec :: Spec
msgSpec = do
    describe "swapSourceAndDest" $ do
        it "swapSourceAndDest actually swaps source" $
            property prop_checkSource
        it "swapSourceAndDest actually swaps dest" $
            property prop_checkDest
        it "swapSourceAndDest . swapSourceAndDest is isomorphic" $
            property prop_isomorphicSourceAndDest
