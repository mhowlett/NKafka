
## NKafka

This is an experimental client for Apache Kafka. I have some radical ideas, they may not work.

It pulls a lot of stuff in from Confluent.Kafka - this is primarily a rewrite of the backend in managed code.

Status: passed second milestone: able to produce a batch of V2 messages successfully. No Consumer.

### Motivation

- How fast can we make a managed client?
  - What if we serialize immediately (avoid long live heap allocations) and
  - Do lazy-reconstruction of messages for delivery reports?
  - Generally avoid the heap.
- Is this easier/harder than wrapping librdkafka?
  - SSL dependency issues, and some interop (eg producev) were very time consuming to get right
  - But so is writing a Kafka client.
  - Re-using code across clients means it is better tested.
  - On the other hand, a C# implementation is much simpler / easier to debug.
  - Also, ease at which users can step into NKafka to debug may help a lot (c.f. librdkafka which is opaque)

### Requirements

- Compatible with Kafka 0.11.0 and above
- Compatible with .NET Standard 1.6 and above
