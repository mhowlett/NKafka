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
    }
}