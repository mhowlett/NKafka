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

        public Buffer GetFreeForUseBuffer(TopicPartition tp, object availableLockObj)
        {
            if (fillingUp.TryGetValue(tp, out Buffer buf))
            {
                return buf;
            }

            lock (availableLockObj)
            {
                if (freeForUse.Count == 0)
                {
                    this.freeForUse.Add(new Buffer(this.bufferSize));
                    this.bufferCount += 1;
                    if (this.bufferCount > 10)
                    {
                        throw new Exception("TODO: reminder to do something better about this");
                    }
                }

                Buffer forUse = freeForUse[freeForUse.Count-1];
                freeForUse.RemoveAt(freeForUse.Count-1);
                forUse.Repurpose(tp, ++this.lastCorrelationId);
                fillingUp.Add(tp, forUse);
                return forUse;
            }
        }

        public void FillingUpToForFinalize(TopicPartition tp)
        {
            var b = fillingUp[tp];
            fillingUp.Remove(tp);
            forFinalize.Add(b);
        }

        public void MoveForFinalizeToForSend(Buffer buffer)
        {
            forFinalize.Remove(buffer);
            forSend.Add(buffer);
        }

        public void ToForFinalizeIfRequired(int lingerMs, int batchSize)
        {
            List<KeyValuePair<TopicPartition, Buffer>> toTransition = null;
            foreach (var kvp in fillingUp)
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

        public void InFlightToFreeForUse(Buffer b)
        {
            inFlight.Remove(b);
            freeForUse.Add(b);
        }

        public Dictionary<TopicPartition, Buffer> FillingUp
        {
            get { return this.fillingUp; }
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
        Dictionary<TopicPartition, Buffer> fillingUp = new Dictionary<TopicPartition, Buffer>();
        List<Buffer> forFinalize = new List<Buffer>();
        List<Buffer> forSend = new List<Buffer>();
        List<Buffer> freeForUse = new List<Buffer>();
        List<Buffer> inFlight = new List<Buffer>();
    }
}