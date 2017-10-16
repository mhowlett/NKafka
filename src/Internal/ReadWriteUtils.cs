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

    }
}

