// Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider, 2017 Matt Howlett
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Derived from: rdkafka-dotnet, licensed under the 2-clause BSD License.
//
// Refer to LICENSE for more information.

using System;


namespace NKafka
{
    /// <summary>
    ///     Enumeration of broker generated error codes.
    /// </summary>
    public enum ErrorCode : Int16
    {
        /// <summary>
        ///     Unknown broker error
        /// </summary>
        Unknown = -1,

        /// <summary>
        ///     Success
        /// </summary>
        NoError = 0,

        /// <summary>
        ///     Offset out of range
        /// </summary>
        OffsetOutOfRange = 1,

        /// <summary>
        ///     Invalid message
        /// </summary>
        InvalidMessage = 2,

        /// <summary>
        ///     Unknown topic or partition
        /// </summary>
        UnknownTopicOrPartition = 3,

        /// <summary>
        ///     Invalid message size
        /// </summary>
        InvalidMessageSize = 4,

        /// <summary>
        ///     Leader not available
        /// </summary>
        LeaderNotAvailable = 5,

        /// <summary>
        ///     Not leader for partition
        /// </summary>
        NotLeaderForPartition = 6,

        /// <summary>
        ///     Request timed out
        /// </summary>
        RequestTimedOut = 7,

        /// <summary>
        ///     Broker not available
        /// </summary>
        BrokerNotAvailable = 8,

        /// <summary>
        ///     Replica not available
        /// </summary>
        ReplicaNotAvailable = 9,

        /// <summary>
        ///     Message size too large
        /// </summary>
        MessageSizeTooLarge = 10,

        /// <summary>
        ///     StaleControllerEpochCode
        /// </summary>
        StaleControllerEpochCode = 11,

        /// <summary>
        ///     Offset metadata string too large
        /// </summary>
        OffsetMetadataTooLarge = 12,

        /// <summary>
        ///     Broker disconnected before response received
        /// </summary>
        NetworkException = 13,

        /// <summary>
        ///     Group coordinator load in progress
        /// </summary>
        GroupLoadInProress = 14,

        /// <summary>
        /// Group coordinator not available
        /// </summary>
        GroupCoordinatorNotAvailable = 15,

        /// <summary>
        ///     Not coordinator for group
        /// </summary>
        NotCoordinatorForGroup = 16,

        /// <summary>
        ///     Invalid topic
        /// </summary>
        TopicException = 17,

        /// <summary>
        ///     Message batch larger than configured server segment size
        /// </summary>
        RecordListTooLarge = 18,

        /// <summary>
        ///     Not enough in-sync replicas
        /// </summary>
        NotEnoughReplicas = 19,

        /// <summary>
        ///     Message(s) written to insufficient number of in-sync replicas
        /// </summary>
        NotEnoughReplicasAfterAppend = 20,

        /// <summary>
        ///     Invalid required acks value
        /// </summary>
        InvalidRequiredAcks = 21,

        /// <summary>
        ///     Specified group generation id is not valid
        /// </summary>
        IllegalGeneration = 22,

        /// <summary>
        ///     Inconsistent group protocol
        /// </summary>
        InconsistentGroupProtocol = 23,

        /// <summary>
        ///     Invalid group.id
        /// </summary>
        InvalidGroupId = 24,

        /// <summary>
        ///     Unknown member
        /// </summary>
        UnknownMemberId = 25,

        /// <summary>
        ///     Invalid session timeout
        /// </summary>
        InvalidSessionTimeout = 26,

        /// <summary>
        ///     Group rebalance in progress
        /// </summary>
        RebalanceInProgress = 27,

        /// <summary>
        ///     Commit offset data size is not valid
        /// </summary>
        InvalidCommitOffsetSize = 28,

        /// <summary>
        ///     Topic authorization failed
        /// </summary>
        TopicAuthorizationFailed = 29,

        /// <summary>
        ///     Group authorization failed
        /// </summary>
        GroupAuthorizationFailed = 30,

        /// <summary>
        ///     Cluster authorization failed
        /// </summary>
        ClusterAuthorizationFailed = 31,

        /// <summary>
        ///     Invalid timestamp
        /// </summary>
        InvalidTimestamp = 32,

        /// <summary>
        ///     Unsupported SASL mechanism
        /// </summary>
        UnsupportedSaslMechanism = 33,

        /// <summary>
        ///     Illegal SASL state
        /// </summary>
        IllegalSaslState = 34,

        /// <summary>
        ///     Unuspported version
        /// </summary>
        UnsupportedVersion = 35
    };

}
