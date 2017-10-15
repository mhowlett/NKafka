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
using System.Diagnostics;
using System.Text;


namespace NKafka
{
    public class ProducerConfig
    {
        public delegate void LoggerDelegate(string message);

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
        ///     TODO: allow override on a per topic level.
        ///     Set the same a max message size.
        /// </summary>
        public Int32 BufferMemoryBytes { get; set; } = 1000012;

        public CompressionType CompressionType { get; set; } = CompressionType.None;

        public Int32 Retries { get; set; } = 0;

        public Int32 BatchSize { get; set; } = 16384;

        public Int32 ConnectionsMaxIdleMs { get; set; } = 540000;

        public Int32 LingerMs { get; set; } = 0;

        public LoggerDelegate Logger { get; set; } = (message) => {};
    }
}
