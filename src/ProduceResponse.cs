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


namespace NKafka
{
    // ProduceResponse => [TopicName [Partition ErrorCode Offset Timestamp]] ThrottleTime
    //   TopicName => string
    //   Partition => int32
    //   ErrorCode => int16
    //   Offset => int64
    //   Timestamp => int64
    //   ThrottleTime => int32  

    public class ProduceResponse
    {
        public class PartitionInfo
        {
            public Int32 Partition;
            public ErrorCode ErrorCode;
            public Int64 Offset;
            public Int64 Timestamp;
        }

        public class TopicInfo
        {
            public string Name;

            public PartitionInfo[] PartitionsInfo;
        }

        public TopicInfo[] TopicsInfo;

        public Int32 ThrottleTime;
        
        public Int32 CorrelationId;
    }
}
