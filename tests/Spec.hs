module Main where

import Test.Hspec
import Raft.LogSpec


main :: IO ()
main = hspec $ do
    logSpec