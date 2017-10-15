using System;

namespace NKafka
{
    internal class Buffer
    {
        public Buffer(int bufferSize)
        {
            this.buffer = new byte[bufferSize];
        }

        public void Repurpose(TopicPartition tp, int correlationId)
        {
            this.topicPartition = tp;
            this.bufferMessageCount = 0;
            this.bufferCurrentPos = 0;
            this.correlationId = correlationId;
        }

        public TopicPartition topicPartition;
        public byte[] buffer;

        public int bufferMessageCount;
        public int bufferCurrentPos;
        public int correlationId;

        // A bunch of offsets into this.buffer that are requred to be populated
        // in order to finalize the message.
        public int requestSizeOffset;
        public int messageSetSizeOffset;
        public int lengthOffset;
        public int crcOffset;
        public int recordCountOffset;
        public int attributesOffset;

        /// <remarks>
        ///     Should be plenty.
        /// </remarks>
        public const int MessageFixedOverhead = 100;

        public override int GetHashCode()
            => this.topicPartition.GetHashCode();
    }
}