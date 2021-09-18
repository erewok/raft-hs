module Main where

import Test.Hspec

import Raft.LogSpec
import Raft.MessageSpec


main :: IO ()
main = hspec $ do
    logSpec
    msgSpec