// Copyright 2017 Matt Howlett, https://www.matthowlett.com
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
// Refer to LICENSE for more information.

using System;
using System.Net;


namespace NKafka
{
    internal unsafe static class ReadWriteUtils
    {
        public enum ApiKeys : Int16
        {
            ProduceRequest = 0,
            FetchRequest = 1,
            OffsetRequest = 2,
            MetadataRequest = 3,
            OffsetCommitRequest = 8,
            OffsetFetchRequest = 9,
            GroupCoordinatorRequest = 10,
            JoinGroupRequest	= 11,
            HeartbeatRequest = 12,
            LeaveGroupRequest = 13,
            SyncGroupRequest = 14,
            DescribeGroupsRequest = 15,
            ListGroupsRequest = 16
        }

        public static byte* WriteString(byte* b, string s)
        {
            if (s == null)
            {
                *((Int16 *)b) = System.Net.IPAddress.HostToNetworkOrder((Int16)(-1)); b += 2;
                return b;
            }

            byte[] bs = System.Text.Encoding.UTF8.GetBytes(s);
            *((Int16 *)b) = System.Net.IPAddress.HostToNetworkOrder((Int16)bs.Length); b += 2;
            for (int i=0; i<bs.Length; ++i)
            {
                *b++ = bs[i];
            }
            return b;
        }



        // -------- Metadata --------

        public static byte* WriteMetadataRequest(byte* b, string topic)
        {
            const Int16 ApiVersion = 0;

            byte* start = b;
            *((Int32 *)b) = 0; b += 4; // Size: fill in later.
            *((Int16 *)b) = System.Net.IPAddress.HostToNetworkOrder((Int16)ApiKeys.MetadataRequest); b += 2; // ApiKey
            *((Int16 *)b) = System.Net.IPAddress.HostToNetworkOrder(ApiVersion); b += 2;
            *((Int32 *)b) = System.Net.IPAddress.HostToNetworkOrder(42); b += 4; // correlationid.
            *((Int16 *)b) = System.Net.IPAddress.HostToNetworkOrder((Int16)2); b += 2; // client id length.
            *(b) = 65; b += 1; // A
            *(b) = 66; b += 1; // B
            *((Int32 *)b) = System.Net.IPAddress.HostToNetworkOrder((Int32)0); b += 4; // topic name count.
            //*((Int16 *)b) = System.Net.IPAddress.HostToNetworkOrder((Int16)2); b += 2; // topic name
            //*(b) = 67; b += 1; // C
            //*(b) = 68; b += 1; // D
            *((Int32 *)start) = System.Net.IPAddress.HostToNetworkOrder((int)(b - start) - sizeof(Int32));
            return b;
        }

        public static MetadataResponse ReadMetadataResponse(byte* b)
        {
            MetadataResponse result = new MetadataResponse();
            Int32 correlationId = System.Net.IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
            Int32 brokerLen = System.Net.IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
            result.BrokerMetadata = new BrokerMetadata[brokerLen];
            for (int i=0; i<brokerLen; ++i)
            {
                result.BrokerMetadata[i] = new BrokerMetadata();
                result.BrokerMetadata[i].NodeId = System.Net.IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                Int16 hostLen = System.Net.IPAddress.NetworkToHostOrder(*((Int16 *)b)); b += 2;
                result.BrokerMetadata[i].Host = System.Text.Encoding.UTF8.GetString(b, hostLen); b += hostLen;
                result.BrokerMetadata[i].Port = System.Net.IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
            }
            Int32 topicsLen = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
            result.TopicMetadata = new TopicMetadata[topicsLen];
            for (int i=0; i<topicsLen; ++i)
            {
                result.TopicMetadata[i] = new TopicMetadata();
                result.TopicMetadata[i].TopicErrorCode = (ErrorCode)IPAddress.NetworkToHostOrder(*((Int16 *)b)); b += 2;
                Int16 topicNameLen = IPAddress.NetworkToHostOrder(*((Int16 *)b)); b += 2;
                result.TopicMetadata[i].TopicName = System.Text.Encoding.UTF8.GetString(b, topicNameLen); b += topicNameLen;
                Int32 partitionsLen = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                result.TopicMetadata[i].PartitionMetadata = new PartitionMetadata[partitionsLen];
                for (int j=0; j<partitionsLen; ++j)
                {
                    result.TopicMetadata[i].PartitionMetadata[j] = new PartitionMetadata();
                    result.TopicMetadata[i].PartitionMetadata[j].PartitionErrorCode = IPAddress.NetworkToHostOrder(*((Int16 *)b)); b += 2;
                    result.TopicMetadata[i].PartitionMetadata[j].PartitionId = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                    result.TopicMetadata[i].PartitionMetadata[j].Leader = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                    Int32 replicasLen = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                    result.TopicMetadata[i].PartitionMetadata[j].Replicas = new Int32[replicasLen];
                    for (int k=0; k<replicasLen; ++k)
                    {
                        result.TopicMetadata[i].PartitionMetadata[j].Replicas[k] = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                    }
                    Int32 isrLen = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                    result.TopicMetadata[i].PartitionMetadata[j].Isr = new Int32[isrLen];
                    for (int k=0; k<isrLen; ++k)
                    {
                        result.TopicMetadata[i].PartitionMetadata[j].Isr[k] = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                    }    
                }
            }
            return result;
        }
        
    }
}

