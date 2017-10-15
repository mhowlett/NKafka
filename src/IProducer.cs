namespace NKafka
{
    public interface IProducer
    {
        void Produce(string topic, byte[] key, byte[] value);
    }
}
