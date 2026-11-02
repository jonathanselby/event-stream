# Event Stream — Engineering Design Document

**Version:** 0.1

## 1. Purpose

Event Stream implements one reusable job: move application events through a bounded embedded stream or Kafka using the same event contract.

> **Embedded when small. Kafka when earned. Same event contract.**

## 2. Non-goals

V0.1 does not provide workflow, business rules, transformation, aggregation, Flink processing, business retry, dead-letter policy, deduplication storage, SQL/outbox persistence, event sourcing, Kafka administration, schema governance or exactly-once business semantics.

Transport reports outcomes. Applications and consumers decide what those outcomes mean.

## 3. Component model

```text
                         Event
                           |
                           v
                      Publisher
                           |
                +----------+----------+
                |                     |
                v                     v
         embedded stream         Kafka producer
                |                     |
                v                     v
       embedded subscriptions       Kafka
                                      |
                             +--------+--------+
                             |                 |
                             v                 v
                        Kafka reader      other consumers
```

Flink, workflow systems and other consumers may consume Kafka records but are not component dependencies.

## 4. Core data model

```go
type Event struct {
    ID            string
    Type          string
    Version       string
    Source        string
    Subject       string
    OccurredAt    time.Time
    CorrelationID string
    CausationID   string
    ContentType   string
    Schema        string
    Key           string
    Payload       []byte
}
```

Required fields are `ID`, `Type`, `Version`, `Source`, `OccurredAt` and `ContentType`.

`Payload` is opaque. `Schema` is a reference only. No business validation is performed.

## 5. Public contracts

```go
type Publisher interface {
    Publish(context.Context, Event) (Receipt, error)
}

type Reader interface {
    Read(context.Context) (Delivery, error)
    Commit(context.Context, Delivery) error
    Close() error
}
```

A successful `Publish` means the selected transport accepted the event according to that transport's contract.

A `Read` may return both a positioned `Delivery` and an envelope error. This lets the caller explicitly decide whether a malformed Kafka record should remain uncommitted or be skipped by committing its position.

## 6. Receipt and delivery

`Receipt` records event ID, transport, destination, partition and offset.

For Kafka, a successful receipt proves broker acknowledgement at the returned topic/partition/offset. It does not prove downstream consumption or business processing.

`Delivery` contains the decoded Event plus transport position. Kafka position includes topic, partition, offset and leader epoch.

## 7. Embedded stream

```text
Publish
   |
   v
bounded ingress
   |
   v
small dispatcher
   |
   +----> bounded subscription A
   |
   +----> bounded subscription B
```

Embedded mode is process-local and memory-backed.

`Publish` succeeds when the event enters bounded ingress; it does not wait for subscribers to process it.

Ingress capacity, default subscription capacity, maximum subscriptions and maximum payload bytes are explicit configuration values.

A slow subscription blocks dispatch. Ingress then fills and later publication returns `ErrBackpressure`. Accepted events are not silently evicted to make space.

Each named subscription receives each event accepted after the subscription exists. V0.1 provides fan-out, not competing embedded consumer groups or a filtering language.

Embedded mode is non-durable. Process termination or a forced shutdown deadline may lose in-memory events.

## 8. Kafka routing

Kafka topology does not enter the Event.

Exact event type/version pairs map to configured topics:

```text
person.safe / 1    -> safety-events
case.approved / 1  -> case-events
```

Missing or duplicate routes are refused. V0.1 has no content-based routing, wildcards, routing expressions or dynamic topic creation.

## 9. Kafka wire mapping

`Event.Key` maps to Kafka record key. `Event.Payload` maps to record value. Envelope metadata maps to Kafka headers.

The mapping uses CloudEvents Kafka binary-mode header names for core attributes where applicable and explicit extension headers for event version, correlation and causation.

The exact mapping is normative in `PROTOCOL.md`.

## 10. Kafka producer

The producer resolves a topic, encodes one record and waits for broker acknowledgement before returning a receipt.

Default producer bounds are:

```text
max payload          1 MiB
max buffered bytes   8 MiB
max buffered records 256
record retries       3 protocol-level attempts
record delivery      10 seconds
required acks        all in-sync replicas
```

All values except required acknowledgements are configurable.

The Kafka client's protocol-level retry is transport behaviour. Event Stream adds no application retry engine.

TLS/SASL and other client connection options are supplied externally through Kafka client options.

## 11. Kafka reader

The reader requires explicit brokers, topics and consumer-group identity.

Defaults bound client fetch buffering to one concurrent fetch, 8 MiB per fetch and 1 MiB per partition request.

```text
Kafka record
     |
     v
decode envelope
     |
     v
Delivery{Event, Topic, Partition, Offset}
     |
     v
caller
     |
explicit Commit
```

Auto-commit is disabled.

The reader holds at most one outstanding delivery. Repeated `Read` calls return that same delivery until it is explicitly committed. This prevents the adapter from advancing past uncommitted work.

Consumer-group rebalancing is blocked while a delivery is outstanding and allowed after successful commit or close. Consumers must therefore complete or deliberately abandon work within their configured group timing budget.

Malformed envelopes return the positioned delivery plus an error and remain uncommitted.

## 12. Offset and ordering semantics

Kafka mode is at least once.

A successful `Read` does not advance committed transport position. `Commit` commits only the currently outstanding delivery.

Ordering is preserved within each partition. V0.1 does not run records from one partition through an additional concurrent scheduling layer.

Parallelism is supplied by Kafka partitions and consumer-group members.

Duplicate delivery remains possible. `Event.ID` is the stable identity available to caller-owned idempotency.

## 13. Security boundary

Kafka TLS and authentication settings are external deployment input.

Credentials never appear in Event, payload metadata, routing tables intended for source control, or receipts.

Customer ACL, IAM, schema-registry and topic-governance policy remain external.

## 14. Observability

`Stats` exposes transport-level counts for publication, failure, read, commit, back-pressure and in-flight work.

No business metrics or observability platform are embedded.

## 15. Failure handling

| Failure | Behaviour |
|---|---|
| invalid envelope | refuse |
| payload over configured limit | refuse |
| missing Kafka route | refuse |
| embedded ingress full | `ErrBackpressure` |
| slow embedded subscriber | back-pressure propagates |
| Kafka unavailable/rejects record | explicit publish error; no receipt |
| authentication failure | explicit transport error |
| malformed Kafka envelope | positioned delivery + error; no implicit commit |
| duplicate delivery | return stable Event ID |
| downstream processing failure | caller concern |
| shutdown deadline | stop within caller bound; embedded in-memory loss is possible |

No business retry, dead-letter or recovery policy exists inside Event Stream.

## 16. Shutdown

Embedded shutdown stops new publication, drains accepted work while possible, and stops at the caller's context deadline.

Kafka publisher shutdown flushes within the supplied context. Kafka reader close releases any blocked rebalance and closes the client.

Shutdown does not claim durable completion of downstream business work.

## 17. State and dependencies

Embedded state is limited to bounded ingress/subscription buffers, subscription registration and lifecycle state.

Kafka state remains with Kafka and the selected client.

The module targets Go 1.26. Core and embedded packages use the standard library. Kafka support uses `franz-go` as the single Kafka client dependency.

No database, persistent queue, retry store, deduplication store, workflow engine or stream-processing runtime is added.

## 18. Operational limits

- embedded mode is non-durable;
- embedded subscriptions are process-local fan-out;
- slow embedded subscribers back-pressure the stream;
- default payload limit is 1 MiB and is configurable;
- Kafka delivery is at least once;
- one Kafka reader exposes one outstanding delivery at a time;
- Kafka routing is exact type/version mapping;
- no automatic business retry or dead-letter policy;
- no business deduplication;
- no automatic replay framework;
- no Kafka administration;
- no stream transformation;
- no cross-broker federation;
- ARM64 is build proof, not a constrained-hardware runtime benchmark.

## 19. Test strategy

Tests prove envelope validation, payload bounds, embedded fan-out/back-pressure/no-discard, exact routing, wire-codec round trip, independent Kafka producer/consumer interoperability, Kafka receipt contents, failed publication behaviour, keyed partition ordering, consumer groups, explicit commit, malformed-envelope handling and shutdown.

Kafka integration tests run against a real Kafka-compatible broker in CI. Independent producer/consumer fixtures use raw `kgo.Client` rather than Event Stream adapters.

CI adds Go 1.26 `gofmt`, vet, race tests, native package/example build, exact version and Linux ARM64 cross-build.

## 20. Acceptance-to-evidence map

| PRD criterion | Executable evidence |
|---|---|
| embedded/Kafka event parity | `TestKafkaCodecRoundTrip`, embedded tests |
| embedded without broker | `TestEmbeddedFanout` |
| bounded embedded resources | `TestEmbeddedBackpressure`, `TestEmbeddedPayloadBound`, `TestEmbeddedMaxSubscriptions` |
| no silent embedded discard | `TestEmbeddedAcceptedEventsNotDiscarded` |
| independent Kafka consume | `TestIndependentConsumerReadsEvent` |
| independent Kafka producer | `TestReaderConsumesIndependentEvent` |
| route resolution | `TestKafkaRouteResolution` |
| missing route refusal | `TestMissingRouteRejected` |
| Kafka receipt | `TestKafkaPublishReceiptAndFailedPublish` |
| keyed partition ordering | `TestKafkaPartitionOrdering` |
| consumer groups/offsets | `TestConsumerGroupOffsets` |
| explicit commit | `TestReadDoesNotImplicitlyCommit` |
| external TLS/auth | public Kafka client-option boundary + protocol docs |
| malformed envelope no implicit commit | `TestMalformedEnvelopeRefusedWithoutImplicitCommit` |
| bounded shutdown | `TestGracefulShutdown`, `TestGracefulShutdownPreservesBufferedSubscriptionEvents` + Kafka close contracts |
| no processing platform | dependency/import review |
| format/vet/race/build/version/ARM64 | `.github/workflows/test.yml` |
