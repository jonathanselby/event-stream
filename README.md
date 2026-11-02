# Event Stream

[![test](https://github.com/jonathanselby/event-stream/actions/workflows/test.yml/badge.svg)](https://github.com/jonathanselby/event-stream/actions/workflows/test.yml)

A small Go event-streaming component with two modes: bounded in-process streaming and native Kafka transport.

> **Embedded when small. Kafka when earned. Same event contract.**

Event Stream moves events. It does not implement workflow, transformation, business retry, deduplication or stream processing.

## What V0.1 provides

- one stable event envelope;
- bounded embedded fan-out with explicit back-pressure;
- exact event type/version to Kafka topic routing;
- Kafka publish receipts with topic, partition and offset;
- Kafka consumer groups with explicit offset commit;
- at-least-once Kafka semantics;
- CloudEvents-compatible Kafka binary metadata mapping;
- configurable payload, producer-buffer and fetch-memory bounds;
- external Kafka TLS/SASL configuration through `franz-go` options;
- Go 1.26 and Linux ARM64 build proof.

## Embedded

```go
stream, _ := embedded.New(embedded.Config{})
sub, _ := stream.Subscribe("worker", 64)

receipt, err := stream.Publish(ctx, event)
delivery, err := sub.Read(ctx)
```

Embedded mode is memory-backed and non-durable. Slow subscribers propagate back-pressure rather than causing silent discard or unbounded growth.

## Kafka

```go
publisher, _ := kafka.NewPublisher(kafka.PublisherConfig{
    Brokers: []string{"kafka:9092"},
    Routes: []kafka.Route{
        {Type: "case.approved", Version: "1", Topic: "case-events"},
    },
})

receipt, err := publisher.Publish(ctx, event)
```

A successful Kafka receipt means the broker acknowledged the event. It does not mean downstream processing succeeded.

The reader uses explicit commit:

```go
delivery, err := reader.Read(ctx)
// application or consumer processes delivery.Event
err = reader.Commit(ctx, delivery)
```

No offset is committed by `Read`.

## Design boundary

```text
business acceptance      event transport       event processing
        |                       |                      |
        v                       v                      v
 application/store         Event Stream         consumer / Flink /
                                                workflow / analytics
```

See [`DESIGN_ASSISTANCE.md`](DESIGN_ASSISTANCE.md).

## Kafka wire contract

The Kafka mapping is documented in [`PROTOCOL.md`](PROTOCOL.md). Event type/version routing is external configuration; topic names do not enter the Event envelope.

## Engineering evidence

- [`PRD.md`](PRD.md)
- [`EDD.md`](EDD.md)
- [`architecture.txt`](architecture.txt)
- [`PROTOCOL.md`](PROTOCOL.md)
- [`DESIGN_ASSISTANCE.md`](DESIGN_ASSISTANCE.md)
- [`generated/EDD.md`](generated/EDD.md)
- [`generated/COMPARISON.md`](generated/COMPARISON.md)
- [`RELEASE.md`](RELEASE.md)

## Limits

V0.1 is deliberately not an ESB, workflow engine, Flink substitute, transactional outbox, Kafka administrator or business retry framework.

## Licence

MIT.
