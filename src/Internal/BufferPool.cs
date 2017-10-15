using System;
using System.Collections.Generic;


namespace NKafka
{
    internal class BufferPool
    {
        private readonly int bufferSize;
        private int bufferCount = 0;

        public BufferPool(int bufferSize)
        {
            this.bufferSize = bufferSize;
        }

        public Buffer Get(TopicPartition tp)
        {
            if (inUse.TryGetValue(tp, out Buffer buf))
            {
                return buf;
            }

            if (available.Count == 0)
            {
                this.available.Add(new Buffer(this.bufferSize));
                this.bufferCount += 1;
                if (this.bufferCount > 10)
                {
                    throw new Exception("TODO: reminder to do something better about this");
                }
            }

            Buffer forUse = available[available.Count-1];
            available.RemoveAt(available.Count-1);

            forUse.bufferMessageCount = 0;
            forUse.bufferCurrentPos = 0;
            forUse.correlationId = ++this.lastCorrelationId;
            
            inUse.Add(tp, forUse);
            return forUse;
        }

        int lastCorrelationId = 0;
        Dictionary<TopicPartition, Buffer> inUse = new Dictionary<TopicPartition, Buffer>();
        List<Buffer> available = new List<Buffer>();
        List<Buffer> waiting = new List<Buffer>();
    }
}