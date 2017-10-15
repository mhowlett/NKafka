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
using System.Net.Sockets;
using System.Text;
using System.Collections.Generic;


namespace NKafka
{
    class Program
    {
        static void ack(ProduceResponse r)
        {
            Console.WriteLine(r.TopicsInfo[0].PartitionsInfo[0].Offset);
        }

        static void Main(string[] args)
        {
            var c = new ProducerConfig
            { 
                BootstrapServers = "localhost:9092",
                ClientId = "test-client",
                RequiredAcks = Acks.One,
                RequestTimeoutMs = 10000
            };

            using (var p = new Producer(c))
            {
                for (int i=0; i<100; ++i)
                {
                    p.Produce("lala", null, Encoding.UTF8.GetBytes("AAAABBBBCCCC"));
                }

                p.Send(ack);
                
                p.Flush(TimeSpan.FromSeconds(10));
            }
        }
    
    }

}
