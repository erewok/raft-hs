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
import Raft.Shared


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


-- | Outbound communication with other nodes using channels
--  If the channel is full we _flush_ the channel and start adding values
--  The idea is that raft should be resilient enough that messages may be lost
--  and the server will merely try again later.


--  Fold over stream of incoming events and check that not too many ticks have
--  come through without a heartbeat received.
