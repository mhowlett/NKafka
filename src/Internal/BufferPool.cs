using System;
using System.Collections.Generic;
using System.Threading.Tasks;


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

        public Buffer GetBuffer(TopicPartition tp, object availableLockObj)
        {
            if (filling.TryGetValue(tp, out Buffer buf))
            {
                return buf;
            }

            lock (availableLockObj)
            {
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
                forUse.Repurpose(tp, ++this.lastCorrelationId);
                filling.Add(tp, forUse);
                return forUse;
            }
        }

        public void MoveToForFinalize(TopicPartition tp)
        {
            var b = filling[tp];
            filling.Remove(tp);
            forFinalize.Add(b);
        }

        public void MoveForFinalizeToForSend(Buffer buffer)
        {
            forFinalize.Remove(buffer);
            forSend.Add(buffer);
        }

        public void TransitionToForFinalize(int lingerMs, int batchSize)
        {
            List<KeyValuePair<TopicPartition, Buffer>> toTransition = null;
            foreach (var kvp in filling)
            {
                if (kvp.Value.bufferMessageCount >= batchSize)
                {
                    if (toTransition == null)
                    {
                        toTransition = new List<KeyValuePair<TopicPartition, Buffer>>();
                    }
                    toTransition.Add(kvp);
                    continue;
                }
            }
        }

        public void MakeAvailable(Buffer b)
        {
            inFlight.Remove(b);
            available.Add(b);
        }

        public Dictionary<TopicPartition, Buffer> Filling
        {
            get { return this.filling; }
        }

        public List<Buffer> ForFinalize
        {
            get { return forFinalize; }
        }

        public List<Buffer> ForSend
        {
            get { return forSend; }
        }

        public List<Buffer> InFlight
        {
            get { return inFlight; }
        }

        int lastCorrelationId = 0;
        Dictionary<TopicPartition, Buffer> filling = new Dictionary<TopicPartition, Buffer>();
        List<Buffer> forFinalize = new List<Buffer>();
        List<Buffer> forSend = new List<Buffer>();
        List<Buffer> available = new List<Buffer>();
        List<Buffer> inFlight = new List<Buffer>();
    }
}