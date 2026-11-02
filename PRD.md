# Event Stream — Product Requirements Document

**Version:** 0.1

## 1. Problem

Applications may need asynchronous event flow for burst handling, internal decoupling or independent consumers. Enterprise deployments may also require native Kafka integration.

Requiring Kafka for all deployments adds unnecessary infrastructure. Supporting only in-process events prevents clean integration with Kafka estates.

The streaming boundary must remain transport-only. Business workflow, transformation, retry policy and other processing logic must not accumulate in the component.

## 2. Product objective

Provide a small Go component that exposes one event contract through an embedded stream or Kafka.

> **Embedded when small. Kafka when earned. Same event contract.**

The V0.1 product question is: can an application move from internal streaming to Kafka without changing its event semantics or introducing an integration platform?

## 3. Users

Applications requiring internal asynchronous events, Kafka publishing or Kafka consumption, and integration teams connecting those events to external consumers.

## 4. In scope

- stable versioned event envelope;
- bounded embedded stream;
- named embedded subscriptions;
- configurable payload and buffer limits;
- Kafka producer;
- Kafka reader;
- externally configured event-to-topic mapping;
- Kafka keys and partitions;
- consumer groups and offsets;
- explicit offset commit;
- broker acknowledgement and publication receipt;
- at-least-once Kafka semantics;
- correlation and causation IDs;
- CloudEvents-compatible Kafka metadata mapping where practical;
- TLS and common Kafka authentication through client configuration;
- transport-level statistics;
- bounded shutdown;
- Go 1.26 and `linux/arm64` build proof.

## 5. Out of scope

- business workflow or rules;
- payload transformation or aggregation;
- Flink or stream processing;
- business retry/backoff;
- dead-letter business policy;
- business deduplication or idempotency;
- SQL persistence or transactional-outbox implementation;
- event sourcing;
- exactly-once business semantics;
- notification fan-out;
- ESB or connector catalogue;
- schema-governance platform;
- Kafka topic/cluster administration;
- customer IAM or ACL policy.

Durable business acceptance, guaranteed event intent and downstream processing remain external concerns.

## 6. Functional requirements

### FR1 — Stable event envelope

Every event has stable identity, type, version, source, occurrence time, content type, payload and optional subject, schema, key, correlation and causation identifiers.

The component validates envelope structure but does not interpret business payloads.

### FR2 — Embedded stream

Events can be published and read asynchronously without external infrastructure.

Ingress and subscription buffers are bounded. Capacity exhaustion produces explicit back-pressure. Embedded mode is non-durable.

### FR3 — Kafka publishing

The same event can be published to Kafka without Kafka-specific fields entering the event envelope.

Event type/version is resolved to a configured topic.

### FR4 — Kafka receipt

A successful Kafka publish returns event ID, topic, partition and offset confirmed by the broker.

Failure returns an explicit error and no successful receipt.

### FR5 — Kafka reading

Events can be read from configured Kafka topics using consumer groups.

Transport position is committed only on explicit caller request.

### FR6 — Delivery semantics

Kafka delivery is at least once.

Duplicate events may occur. Stable event IDs allow consumers to implement business idempotency where required.

### FR7 — Partition ordering

Ordering is preserved within a Kafka partition.

V0.1 does not introduce a second scheduling or offset-coordination layer over Kafka.

### FR8 — Resource bounds

Embedded buffers, payload size, Kafka producer buffering and Kafka fetch memory are explicitly bounded and configurable.

### FR9 — Enterprise security

Kafka TLS and supported authentication configuration are external to the event model.

Credentials never form part of an event or receipt.

### FR10 — Transport observability

The component exposes enough transport state to identify successful and failed publication, reads, commits, back-pressure and in-flight work.

It does not embed an observability platform.

## 7. Acceptance criteria

V0.1 is accepted when executable evidence demonstrates:

1. the same event contract operates in embedded and Kafka modes;
2. embedded mode requires no broker;
3. embedded memory, subscription count and payload size are bounded;
4. capacity exhaustion produces explicit back-pressure;
5. accepted embedded events are not silently discarded during normal operation;
6. Kafka producer output is consumable by an independent client;
7. the Kafka reader consumes independently produced compatible events;
8. event type/version resolves to externally configured topics;
9. a successful Kafka publish returns topic, partition and offset;
10. a failed Kafka publish returns no successful receipt;
11. keys and partition ordering behave as documented;
12. consumer groups and offsets behave as documented;
13. transport position is not committed before explicit caller commit;
14. TLS/authentication remains external to domain code;
15. malformed envelopes are refused without implicit commit;
16. no SQL store, workflow, retry engine, deduplication store, Flink runtime, ESB or stream-processing engine is embedded;
17. shutdown remains bounded by caller context;
18. `gofmt`, `go vet`, race tests, native build and `linux/arm64` build pass.

## 8. Definition of done

V0.1 is complete when embedded and Kafka interoperability are executable, publication receipts and failure semantics are explicit, resource use is bounded, every acceptance criterion has named evidence, and no business-processing responsibility has entered the transport boundary.
