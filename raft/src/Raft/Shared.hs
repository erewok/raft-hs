{-# LANGUAGE DeriveGeneric                 #-}

module Raft.Shared where

import Data.Hashable (Hashable)
import GHC.Generics (Generic)


-- | Every raft server has an Id, which is represented as an `int` here
newtype ServerId = ServerId { unServerId :: Int } deriving (Show, Eq, Generic)
instance Hashable ServerId