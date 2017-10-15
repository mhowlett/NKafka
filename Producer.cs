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
using System.Net;


namespace NKafka
{
    public unsafe class Producer : IDisposable
    {
        private ProducerConfig config;
        private Client client;

        public Producer(ProducerConfig config)
        {
            this.config = config;

            this.client = new Client(config.BootstrapServers);
            this.buffer = new byte[config.BufferMemoryBytes];
            this.bufferMessageCount = 0;
            this.bufferCurrentPos = 0;
        }

        public delegate void ack(ProduceResponse r);

        private object producerLock = new object();

        private byte[] buffer;
        private int bufferMessageCount;
        private int bufferCurrentPos;

        private int RequestSizeOffset;
        private int MessageSetSizeOffset;
        private int LengthOffset;
        private int CrcOffset;
        private int RecordCountOffset;
        private int AttributesOffset;

        public unsafe void Produce(string topic, byte[] key, byte[] value, ack ack)
        {
            lock (producerLock)
            {
                if (bufferCurrentPos == 0)
                {
                    fixed (byte *b = this.buffer)
                    {
                        RequestSizeOffset = 0;
                        byte *currentPosPtr = this.WriteProduceRequestHeader(b, topic);
                        MessageSetSizeOffset = (int)(currentPosPtr-b);
                        currentPosPtr += 4; // space for message set size.

                        byte* lengthPtr;
                        byte* crcPtr;
                        byte* recordCountPtr;
                        byte* attributesPtr;
                        currentPosPtr = this.WriteRecordBatchHeader(currentPosPtr, out lengthPtr, out crcPtr, out attributesPtr, out recordCountPtr);

                        LengthOffset = (int)(lengthPtr-b);
                        CrcOffset = (int)(crcPtr-b);
                        AttributesOffset = (int)(attributesPtr-b);
                        RecordCountOffset = (int)(recordCountPtr-b);

                        bufferCurrentPos = (int)(currentPosPtr-b);
                    }
                }

                fixed (byte *b = this.buffer)
                {
                    byte* start = b + bufferCurrentPos;
                    byte* currentPosPtr = WriteRecord(start, key, value);

                    byte* requestSizePtr = b + RequestSizeOffset;
                    byte* messageSetSizePtr = b + MessageSetSizeOffset;
                    byte* lengthPtr = b + LengthOffset;
                    byte* crcPtr = b + CrcOffset;
                    byte* recordCountPtr = b + RecordCountOffset;
                    byte* attributesPtr = b + AttributesOffset;

                    *((Int32 *)messageSetSizePtr) = IPAddress.HostToNetworkOrder((int)(currentPosPtr - messageSetSizePtr) - 4);  
                    *((Int32 *)requestSizePtr) = IPAddress.HostToNetworkOrder((int)(currentPosPtr - b) - 4);
                    *((Int32 *)lengthPtr) = IPAddress.HostToNetworkOrder((int)(currentPosPtr - lengthPtr - 4));
                    *((Int32 *)recordCountPtr) = IPAddress.HostToNetworkOrder((int)1);
                    var crc = Crc32Provider.ComputeHash(attributesPtr, 0, (int)(currentPosPtr-attributesPtr));
                    for (int i=0; i<crc.Length; ++i) *crcPtr++ = crc[i];

                    bufferCurrentPos = (int)(currentPosPtr - b);
                }

                client.Send(this.buffer, this.bufferCurrentPos);

                fixed (byte *b = client.Receive())
                {
                    ack(ReadProduceResponse(b));
                }
            }
        }

        public void Send()
        {

        }

        public void Flush(TimeSpan timeoutMilliseconds)
        {
        }

        public void Dispose()
        {
            lock (producerLock)
            {
                client.Dispose();
            }
        }

        public byte* WriteProduceRequestHeader(byte* b, string topic)
        {
            const Int16 ApiVersion = 3;

            // RequestOrResponse => Size (RequestMessage | ResponseMessage)
            //   Size => int32

            // RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
            //   ApiKey => int16
            //   ApiVersion => int16
            //   CorrelationId => int32
            //   ClientId => string
            //   RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest

            // ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
            //   TransactionalId => string [evidently]
            //   RequiredAcks => int16
            //   Timeout => int32
            //   Partition => int32
            //   MessageSetSize => int32

            byte* start = b;
            *((Int32 *)b) = 0; b += 4;                                                 // Size: fill in at end.
            *((Int16 *)b) = IPAddress.HostToNetworkOrder((Int16)ReadWriteUtils.ApiKeys.ProduceRequest); b += 2;  // ApiKey
            *((Int16 *)b) = IPAddress.HostToNetworkOrder(ApiVersion); b += 2;          // ApiVersion
            *((Int32 *)b) = IPAddress.HostToNetworkOrder((Int32)3); b += 4;            // CorrelationId
            for (int i=0; i<this.config.clientId.Length; ++i)                          // ClientId
            {
                *b++ = this.config.clientId[i];
            }
            b = ReadWriteUtils.WriteString(b, null);                                   // TransactionalId - WTF?
            *((Int16 *)b) = IPAddress.HostToNetworkOrder((Int16)(1)); b += 2;          // RequiredAcks
            *((Int32 *)b) = IPAddress.HostToNetworkOrder((Int32)5000); b += 4;         // Timeout
            *((Int32 *)b) = IPAddress.HostToNetworkOrder((Int32)1); b += 4;            // TopicCount
            b = ReadWriteUtils.WriteString(b, topic);                                  // TopicName
            *((Int32 *)b) = IPAddress.HostToNetworkOrder((Int32)1); b += 4;            // PartitionCount
            *((Int32 *)b) = IPAddress.HostToNetworkOrder((Int32)0); b += 4;            // Partition

            return b;
        }

        private byte* WriteRecordBatchHeader(byte *b, out byte* length, out byte* crcStart, out byte* attributesOffset, out byte* recordCount)
        {
            long now = Timestamp.DateTimeToUnixTimestampMs(DateTime.Now);

            // RecordBatch =>
            //   FirstOffset => int64
            //   Length => int32 (in bytes)
            //   PartitionLeaderEpoch => int32
            //   Magic => int8
            //   CRC => int32
            //   Attributes => int16
            //   LastOffsetDelta => int32
            //   FirstTimestamp => int64
            //   MaxTimestamp => int64
            //   ProducerId => int64
            //   ProducerEpoch => int16
            //   FirstSequence => int32
            //   Records => [Record]

            const byte MagicValue = 2;

            byte* start = b;
            *((Int64 *)b) = 0; b += 8;                                    // FirstOffset
            length = b;
            *((Int32 *)b) = 0; b += 4;                                    // Length (in bytes, update later)
            *((Int32 *)b) = 0; b += 4;                                    // PartitionLeaderEpoch
            *b++ = MagicValue;                                            // Magic
            crcStart = b;
            *((Int32 *)b) = 0; b += 4;                                    // CRC (updated later)
            attributesOffset = b;
            *((Int16 *)b) = 0; b += 2;                                    // Attributes
            *((Int32 *)b) = 0; b += 4;                                    // LastOffsetDelta (with one message, will be 0)
            *((Int64 *)b) = IPAddress.HostToNetworkOrder(now); b += 8;    // FirstTimestamp
            *((Int64 *)b) = IPAddress.HostToNetworkOrder(now); b += 8;    // MaxTimestamp
            *((Int64 *)b) = -1; b += 8;                                   // ProducerId (not required unless implementing idempotent)
            *((Int16 *)b) = 0;  b += 2;                                   // ProducerEpoch (not required unless implementing idempotent)
            *((Int32 *)b) = IPAddress.HostToNetworkOrder(-1); b += 4;     // FirstSequence (not required unless implementing idempotent)
            recordCount = b;
            *((Int32 *)b) = IPAddress.HostToNetworkOrder(0); b += 4;      // Record count (update later)
            
            return b;
        }

        private static byte* WriteRecordBatch(byte *b, byte[] key, byte[] value)
        {
            long now = Timestamp.DateTimeToUnixTimestampMs(DateTime.Now);

            // RecordBatch =>
            //   FirstOffset => int64
            //   Length => int32 (in bytes)
            //   PartitionLeaderEpoch => int32
            //   Magic => int8
            //   CRC => int32
            //   Attributes => int16
            //   LastOffsetDelta => int32
            //   FirstTimestamp => int64
            //   MaxTimestamp => int64
            //   ProducerId => int64
            //   ProducerEpoch => int16
            //   FirstSequence => int32
            //   Records => [Record]

            const byte MagicValue = 2;

            byte* start = b;
            *((Int64 *)b) = 0; b += 8;                                    // FirstOffset
            byte* length = b;
            *((Int32 *)b) = 0; b += 4;                                    // Length (in bytes, update later)
            *((Int32 *)b) = 0; b += 4;                                    // PartitionLeaderEpoch
            *b++ = MagicValue;                                            // Magic
            byte* crcStart = b;
            *((Int32 *)b) = 0; b += 4;                                    // CRC (updated later)
            byte* attributesOffset = b;
            *((Int16 *)b) = 0; b += 2;                                    // Attributes
            *((Int32 *)b) = 0; b += 4;                                    // LastOffsetDelta (with one message, will be 0)
            *((Int64 *)b) = IPAddress.HostToNetworkOrder(now); b += 8;    // FirstTimestamp
            *((Int64 *)b) = IPAddress.HostToNetworkOrder(now); b += 8;    // MaxTimestamp
            *((Int64 *)b) = -1; b += 8;                                   // ProducerId (not required unless implementing idempotent)
            *((Int16 *)b) = 0;  b += 2;                                   // ProducerEpoch (not required unless implementing idempotent)
            *((Int32 *)b) = IPAddress.HostToNetworkOrder(-1); b += 4;     // FirstSequence (not required unless implementing idempotent)
            *((Int32 *)b) = IPAddress.HostToNetworkOrder(1); b += 4;      // Record count.
            b = WriteRecord(b, key, value);
            *((Int32 *)length) = IPAddress.HostToNetworkOrder((int)(b - length - 4));
            var crc = Crc32Provider.ComputeHash(attributesOffset, 0, (int)(b-attributesOffset));
            for (int i=0; i<crc.Length; ++i) *crcStart++ = crc[i];

            return b;
        }

        private static unsafe byte* WriteRecord(byte *b, byte[] key, byte[] value)
        {
            // Record =>
            //   Length => varint
            //   Attributes => int8
            //   TimestampDelta => varint
            //   OffsetDelta => varint
            //   KeyLen => varint
            //   Key => data
            //   ValueLen => varint
            //   Value => data
            //   Headers => [Header]

            byte* lengthMem = stackalloc byte[10];
            byte* timestampDeltaMem = stackalloc byte[10];
            byte* offsetDeltaMem = stackalloc byte[10];
            byte* keyLenMem = stackalloc byte[10];
            byte* valueLenMem = stackalloc byte[10];

            int keyLen = key == null ? -1 : key.Length;
            int valueLen = value == null ? -1 : value.Length;

            VarintBitConverter.GetVarintBytes(timestampDeltaMem, 0);
            VarintBitConverter.GetVarintBytes(offsetDeltaMem, 0);
            VarintBitConverter.GetVarintBytes(keyLenMem, keyLen);
            VarintBitConverter.GetVarintBytes(valueLenMem, valueLen);

            // TODO: serialize headers here. this is annoying, since we can't do it inline.
            // we may be able to use a heuristic which is correct 90% of the time or something.

            int length = 
                1 +                     // Attributes
                *timestampDeltaMem +    // timestampDelta (first byte is length of varint)
                *offsetDeltaMem +       // OffsetDelta
                *keyLenMem +            // key length
                (key == null ? 0 : keyLen) +                // key
                *valueLenMem +          // value length
                (value == null ? 0 : valueLen) +              // value
                1;                      // Header count bytes ?
            
            VarintBitConverter.GetVarintBytes(lengthMem, length);

            for (int i=0; i<*lengthMem; ++i) *b++ = lengthMem[i+1];                  // Length
            *b++ = 0;                                                                // Attrbiutes
            for (int i=0; i<*timestampDeltaMem; ++i) *b++ = timestampDeltaMem[i+1];  // TimestampDelta
            for (int i=0; i<*offsetDeltaMem; ++i) *b++ = offsetDeltaMem[i+1];        // OffsetDelta
            for (int i=0; i<*keyLenMem; ++i) *b++ = keyLenMem[i+1];                  // key length
            if (key != null)
            {
                for (int i=0; i<key.Length; ++i) *b++ = key[i];                      // key
            }
            for (int i=0; i<*valueLenMem; ++i) *b++ = valueLenMem[i+1];              // value length
            if (value != null)
            {
                for (int i=0; i<value.Length; ++i) *b++ = value[i];                  // value
            }
            *((byte *)b) = 0; b += 1;                                                // Headers Length is in bytes ? 
            return b;
        }

        private static byte* WriteHeader(byte *b)
        {
            // Header => HeaderKey HeaderVal
            //   HeaderKeyLen => varint
            //   HeaderKey => string
            //   HeaderValueLen => varint
            //   HeaderValue => data
            return b;
        }

        public static ProduceResponse ReadProduceResponse(byte* b)
        {
            // ProduceResponse => [TopicName [Partition ErrorCode Offset Timestamp]] ThrottleTime
            //   TopicName => string
            //   Partition => int32
            //   ErrorCode => int16
            //   Offset => int64
            //   Timestamp => int64
            //   ThrottleTime => int32
            
            ProduceResponse result = new ProduceResponse();
            Int32 correlationId = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
            Int32 topicsLen = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
            result.TopicsInfo = new ProduceResponse.TopicInfo[topicsLen];
            for (int i=0; i<topicsLen; ++i)
            {
                result.TopicsInfo[i] = new ProduceResponse.TopicInfo();
                Int16 topicLen = IPAddress.NetworkToHostOrder(*((Int16 *)b)); b += 2;
                result.TopicsInfo[i].Name = System.Text.Encoding.UTF8.GetString(b, topicLen); b += topicLen;
                Int32 partitionsLen = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                result.TopicsInfo[i].PartitionsInfo = new ProduceResponse.PartitionInfo[partitionsLen];
                for (int j=0; j<partitionsLen; ++j)
                {
                    result.TopicsInfo[i].PartitionsInfo[j] = new ProduceResponse.PartitionInfo();
                    result.TopicsInfo[i].PartitionsInfo[j].Partition = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
                    result.TopicsInfo[i].PartitionsInfo[j].ErrorCode = (ErrorCode)IPAddress.NetworkToHostOrder(*((Int16 *)b)); b += 2;
                    result.TopicsInfo[i].PartitionsInfo[j].Offset = IPAddress.NetworkToHostOrder(*((Int64 *)b)); b += 8;
                    result.TopicsInfo[i].PartitionsInfo[j].Timestamp = IPAddress.NetworkToHostOrder(*((Int64 *)b)); b += 8;
                }
            }
            result.ThrottleTime = IPAddress.NetworkToHostOrder(*((Int32 *)b)); b += 4;
            return result;
        }

    }
}
