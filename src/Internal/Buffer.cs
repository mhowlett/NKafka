using System;

namespace NKafka
{
    internal class Buffer
    {
        public Buffer(int bufferSize)
        {
            this.buffer = new byte[bufferSize];
        }

        public byte[] buffer;

        public int bufferMessageCount;
        public int bufferCurrentPos;
        public DateTime lastSent;
        public int correlationId;

        // A bunch of offsets into this.buffer that are requred to be populated
        // in order to finalize the message.
        public int requestSizeOffset;
        public int messageSetSizeOffset;
        public int lengthOffset;
        public int crcOffset;
        public int recordCountOffset;
        public int attributesOffset;
    }
}