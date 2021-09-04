module Raft.LogSpec where

import Test.Hspec
import Test.Hspec.QuickCheck (prop)

import Raft.Log



spec :: Spec
spec = do
  describe "sort" $ do
    it "is idempotent" $
      prop $ \xs -> sort (sort xs :: [Int]) == sort xs
    it "is identity" $ -- not really
      LC.property $ \xs -> sort (xs :: [Int]) == xs