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
using System.Collections.Generic;
using System.Text;


namespace NKafka
{
    public class ProducerConfig
    {
        public string BootstrapServers { get; set; } = "localhost:9092";

        internal byte[] clientId = new byte[] { 0, 6, 78, 75, 97, 102, 107, 97 }; // "NKafka"

        public unsafe string ClientId 
        {
            get
            {
                return Encoding.UTF8.GetString(clientId, 2, clientId.Length - 2);
            }
            set
            {
                clientId = new byte[Encoding.UTF8.GetBytes(value).Length + 2];
                fixed (byte* b = clientId)
                {
                    ReadWriteUtils.WriteString(b, value);
                }
            }
        }

        public Acks RequiredAcks { get; set; } = Acks.One;

        public Int32 RequestTimeoutMs { get; set; } = 30000;

        /// <summary>
        ///     The total bytes of memory the producer can use to buffer records waiting 
        ///     to be sent to the server. If records are sent faster than they can be 
        ///     delivered to the server the producer will block for max.block.ms after 
        ///     which it will throw an exception.
        /// 
        ///     This setting should correspond roughly to the total memory the producer 
        ///     will use, but is not a hard bound since not all memory the producer uses 
        ///     is used for buffering. Some additional memory will be used for compression 
        ///     (if compression is enabled) as well as for maintaining in-flight requests.
        /// </summary>
        public Int32 BufferMemoryBytes { get; set; } = 33554432;

        /// <summary>
        ///     Compression not currently implemented.
        /// </summary>
        public CompressionType CompressionType { get; set; } = CompressionType.None;

        public Int32 Retries { get; set; } = 0;

        public Int32 BatchSize { get; set; } = 16384;

        public Int32 ConnectionsMaxIdleMs { get; set; } = 540000;

        public Int32 LingerMs { get; set; } = 0;
    }
}
