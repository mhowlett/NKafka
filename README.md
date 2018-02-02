## NKafka

This is an experimental .NET client for Apache Kafka.

It pulls a lot of stuff in from Confluent.Kafka - this is primarily a rewrite of the backend in managed code.

### Status

Able to produce batches of V2 messages, with multiple requests in flight, managed by a background thread. Metadata
requests are implemented but not incorporated yet. Only works with a single partition. Definitely not yet ready
for prime time.

No Consumer yet.

### Benchmarks 

I ran a simple test producing 5,000,000 100 byte messages to localhost on my mac book pro. 

Results:

- NKafka: 170k messages/s
- Confluent.Kafka [Task]: 220k messages/s
- Confluent.Kafka [DeliveryHandler]: 320k messages/s


### Example

```
    static void ack(ProduceResponse r)
    {
        Console.WriteLine(r.TopicsInfo[0].PartitionsInfo[0].Offset);
    }

    static void Main(string[] args)
    {
        var c = new ProducerConfig
        { 
            BootstrapServers = "localhost:9092",
            ClientId = "test-client",
            RequiredAcks = Acks.One,
            RequestTimeoutMs = 10000
        };

        using (var p = new Producer(c, ack))
        {
            for (int i=0; i<20; ++i)
            {
                p.Produce("test-topic", null, Encoding.UTF8.GetBytes("AAAABBBBCCCC"));
            }

            p.Flush(TimeSpan.FromSeconds(10));
        }
    }
```

Better delivery report functionality coming soon.

### Motivation

- How fast can we make a managed client?
  - What if we serialize immediately (avoid long live heap allocations) and
  - Do lazy-reconstruction of messages for delivery reports?
  - Generally avoid the heap.
- Is this easier/harder than wrapping librdkafka?
  - SSL dependency issues, and some interop (eg producev) are time consuming to get right.
  - But so is writing a Kafka client.
  - Re-using code across clients means it is better tested.
  - On the other hand, a C# implementation is much simpler / easier to debug.
  - Also, ease at which users can step into NKafka to debug may help a lot (c.f. librdkafka which is opaque)

### Requirements

- Compatible with Kafka 0.11.0 and above
- Compatible with .NET Standard 1.6 and above
