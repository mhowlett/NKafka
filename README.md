
## NKafka

this is an experimental client for Apache Kafka.

Currently at second milestone: able to produce a batch of V2 messages successfully.

### Motivation

- How fast can we make a managed client?
  - What if we serialize immediately (avoid long live heap allocations) and
  - Do lazy-reconstruction of messages for delivery reports?
  - Generally avoid the heap.
- Is this easier/harder than wrapping librdkafka?
  - SSL issues, and some interop (eg producev) were very time consuming
  - But so is writing a Kafka client.

### Requirements

- Compatible with Kafka 0.11.0 and above
- Compatible with .NET Standard 1.6 and above
