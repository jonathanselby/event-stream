# Event Stream — Generated Engineering Design Evidence

**Source:** implementation recovery  
**Evidence commit:** `ba07d158c9789a105f2a63c746719d1d6683b493`

## Recovered component boundary

The implementation contains three runtime packages:

```text
eventstream
  Event / Receipt / Delivery / Publisher / Reader / Stats
        |
        +--> embedded
        |      bounded ingress + named fan-out subscriptions
        |
        +--> kafka
               exact route table + Kafka codec + producer + reader
```

No database, workflow engine, transformation pipeline, business retry store, deduplication store or Kafka administration package exists.

## Recovered core contract

`Event` carries transport-neutral identity, type, version, source, optional subject, occurrence time, correlation/causation IDs, content type, schema reference, key and opaque payload.

Envelope validation requires ID, type, version, source, occurrence time and content type. Payload-size enforcement is transport-configurable.

`Publisher.Publish` returns `Receipt` or error. `Reader.Read` returns `Delivery`; `Reader.Commit` explicitly advances transport position.

## Recovered embedded behaviour

`embedded.Stream` owns:

- one bounded ingress channel;
- bounded named subscription channels;
- configured maximum subscription count;
- configured payload limit;
- one dispatcher goroutine;
- explicit back-pressure when ingress is full;
- no persistence.

Publication succeeds once the event is accepted into ingress. The dispatcher fans each accepted event to every active subscription. A blocked subscription blocks the dispatcher, then fills ingress and propagates back-pressure.

Accepted events are not evicted to make room. Graceful shutdown stops new publication and waits for dispatched ingress work until the caller deadline. Events already buffered in a subscription remain readable after stream closure.

## Recovered Kafka routing and wire contract

`kafka.Router` maps exact `(Type, Version)` pairs to topics. Duplicate routes and missing routes are refused.

The Kafka codec maps:

- `Event.Key` to record key;
- `Event.Payload` to record value;
- CloudEvents-compatible `ce_*` headers for ID/type/source/subject/time/schema;
- `content-type` for payload media type;
- explicit `event-version`, `correlation-id` and `causation-id` headers.

The decoder refuses duplicate headers, unsupported CloudEvents version, invalid time and incomplete required envelopes.

## Recovered Kafka producer behaviour

`kafka.Publisher`:

1. validates envelope and payload bound;
2. resolves exact route;
3. encodes one Kafka record;
4. synchronously waits for broker result;
5. returns topic, partition and offset only on success.

Defaults enforce all-ISR acknowledgements, bounded producer buffering, a 10-second record delivery budget and three protocol-level record retries.

No application retry policy is implemented.

## Recovered Kafka reader behaviour

`kafka.Reader` configures explicit topics, consumer group, disabled auto-commit, blocked rebalance while a record is outstanding and bounded fetch memory.

It holds one outstanding delivery at a time. Repeated reads return the same delivery until successful explicit commit. Commit must match the outstanding topic/partition/offset and then allows group rebalancing.

A malformed envelope still returns its transport position plus an error and remains uncommitted until the caller explicitly commits or closes.

## Recovered state and dependencies

Core and embedded code use the Go standard library.

Kafka support has one direct external dependency: `github.com/twmb/franz-go`.

Runtime state is bounded memory or Kafka/client state. No persistent application state exists in the component.

## Recovered evidence surface

Tests cover:

- core envelope and payload bounds;
- embedded fan-out, maximum subscriptions, back-pressure, no silent discard and shutdown;
- Kafka route validation and codec round trip;
- independent producer/consumer interoperability against a real Kafka-compatible broker in CI;
- publication receipts and missing-route failure;
- keyed partition ordering;
- explicit commit and consumer-group resume;
- malformed envelope without implicit commit.

CI adds format, vet, race, example builds, version contract and Linux ARM64 build proof.
