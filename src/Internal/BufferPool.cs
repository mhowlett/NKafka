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

        public Buffer GetTopicPartitionBuffer_Blocking(TopicPartition tp, object freeForUseLockObj)
        {
            bool waited = false;
            while (true)
            {
                var result = GetTopicPartitionBuffer(tp, freeForUseLockObj);
                if (result != null)
                {
                    if (waited)
                    {
                        // Console.WriteLine("blocked on buffer get");
                    }
                    return result;
                }
                waited = true;
                Task.Delay(TimeSpan.FromMilliseconds(50)).Wait();
            }
        }

        public Buffer GetTopicPartitionBuffer(TopicPartition tp, object freeForUseLockObj)
        {
            if (fillingUp.TryGetValue(tp, out Buffer buf))
            {
                return buf;
            }

            lock (freeForUseLockObj)
            {
                if (freeForUse.Count == 0)
                {
                    if (this.bufferCount > 50)
                    {
                        return null;
                    }
                    this.bufferCount += 1;
                    this.freeForUse.Add(new Buffer(this.bufferSize));
                }

                Buffer forUse = freeForUse[freeForUse.Count-1];
                freeForUse.RemoveAt(freeForUse.Count-1);
                forUse.Repurpose(tp, ++this.lastCorrelationId);
                fillingUp.Add(tp, forUse);
                return forUse;
            }
        }

        public void Move_FillingUp_To_ForFinalize(TopicPartition tp)
        {
            var b = fillingUp[tp];
            fillingUp.Remove(tp);
            forFinalize.Add(b);
        }

        public void Move_InFlight_To_FreeForUse(Buffer b)
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