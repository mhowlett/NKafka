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
        public string BootstrapServers { get; set; }

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

        public Acks RequiredAcks { get; set; }

        public Int32 MaxMessageSize { get; set; }

        public Int32 RequestTimeoutMilliseconds { get; set; }

        public Partitioner Partitioner { get; set; }
    }
}
