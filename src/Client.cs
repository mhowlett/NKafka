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


namespace NKafka
{
    public class Client : IDisposable
    {
        private Socket socket; 

        public Client(string bootstrapServer)
        {
            String[] parts = bootstrapServer.Split(':');
            socket = ConnectSocket(parts[0], int.Parse(parts[1]));
            if (this.socket == null)
            {
                throw new Exception("connection to bootstrap server failed");
            }
        }

        private Socket ConnectSocket(string server, int port)
        {
            IPHostEntry hostEntry = Dns.GetHostEntryAsync(server).Result;

            // Loop through the AddressList to obtain the supported AddressFamily. This is to avoid
            // an exception that occurs when the host IP Address is not compatible with the address family
            // (typical in the IPv6 case).
            Socket result = null;
            foreach(IPAddress address in hostEntry.AddressList)
            {
                IPEndPoint ipe = new IPEndPoint(address, port);
                Socket tempSocket = new Socket(
                    ipe.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

                tempSocket.Connect(ipe);

                if (tempSocket.Connected)
                {
                    result = tempSocket;
                    break;
                }
            }
            return result;
        }

        private static int DeserializeInt(byte[] data)
        {
            // network byte order -> big endian -> most significant byte in the smallest address.
            return
                (((int)data[0]) << 24) |
                (((int)data[1]) << 16) |
                (((int)data[2]) << 8) |
                (int)data[3];
        }

        public void Send(byte[] bs, int size)
        {
            // Send request to the server.
            this.socket.Send(bs, 0, size, SocketFlags.None);
        }

        public bool DataToReceive
        {
            get 
            {
                return this.socket.Available > 0;
            }
        }

        public byte[] Receive()
        {
            int bytes = 0;

            Byte[] bytesReceived = new Byte[4096];
            Byte[] sizeBuf = new Byte[4];
            int curr = 0;
            int len = 0;
            do {
                bytes = this.socket.Receive(sizeBuf, 0, 4, 0);
                len = DeserializeInt(sizeBuf);
                bytes = this.socket.Receive(bytesReceived, curr, len-curr, 0);
                curr = curr + bytes;
            }
            while (len - curr > 0);

            var result = new byte[len];
            for (int i=0; i<len; ++i)
            {
                result[i] = bytesReceived[i];
            }
            return result;
        }

        public void Dispose()
        {
            socket.Dispose();
        }

        private unsafe static byte* WriteMetadataRequest(byte* b, string topic)
        {
            const Int16 ApiVersion = 0;

            byte* start = b;
            *((Int32 *)b) = 0; b += 4; // Size: fill in later.
            *((Int16 *)b) = System.Net.IPAddress.HostToNetworkOrder((Int16)ReadWriteUtils.ApiKeys.MetadataRequest); b += 2; // ApiKey
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

        private unsafe static MetadataResponse ReadMetadataResponse(byte* b)
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