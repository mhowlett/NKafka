
## NKafka

This might become a client for Apache Kafka.

Currently at first milestone: able to produce a single V2 message successfully.

### Motivation

- How fast can we make a managed client?
  - What if we serialize immediately (avoid long live heap allocations) and
  - Do lazy-reconstruction of messages for delivery reports?
  - Generally avoid the heap.
- Is this easier/harder than wrapping librdkafka?
  - SSL issues, and some interop (eg producev) were very time consuming.

### Requirements

- Compatible with Kafka 0.11.0 and above
- Compatible with .NET Core 1.6 and above
