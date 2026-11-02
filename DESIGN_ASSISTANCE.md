# Event Stream — Design Assistance

**Version:** 0.1

## 1. Purpose

Event Stream owns transport.

This document identifies where common adjacent responsibilities should sit so business logic does not migrate into the transport layer.

> **Accept state where state belongs. Transport events where transport belongs. Process events where processing belongs.**

## 2. Responsibility model

```text
business acceptance      event transport       event processing
        |                       |                      |
        v                       v                      v
 application/store         Event Stream         consumer / Flink /
                                                workflow / analytics
```

## 3. Durable business acceptance

A user-facing action that must not be lost should be durably accepted before success is reported.

Typical pattern:

```text
request
   |
validate
   |
durable business state
+ durable event intent where required
   |
commit
   |
success to caller
```

Event Stream does not implement the durable store or event-intent mechanism.

If publication fails, it reports failure. Recovery of a durable pending event is owned by the application mechanism surrounding Event Stream.

## 4. Kafka integration

Domain events should not contain customer Kafka topology.

```text
application event             deployment mapping
-----------------             ------------------
type: case.approved     -->   topic: customer-case-events
version: 1                    key: case_id
```

An integration contract should document event type/version, Kafka topic, record key, payload/content type, schema reference where used, expected retention and security/configuration ownership.

Topic provisioning, retention, replication and ACLs remain with the Kafka estate.

## 5. Publication receipt

A Kafka receipt records event ID, topic, partition and offset.

It proves Kafka acknowledged publication. It does not prove a consumer processed the event.

Applications may persist receipts where audit requirements justify it. Event Stream does not provide receipt storage.

## 6. Failure ownership

| Failure | Owner |
|---|---|
| invalid user/business request | application |
| durable business write failure | application |
| embedded back-pressure | publishing application |
| Kafka publication failure | publishing application |
| recovery of durable pending publication | application |
| malformed transport envelope | Event Stream reports; caller decides |
| downstream business failure | consumer |
| processing retry | consumer |
| transformation/aggregation failure | stream processor |
| duplicate business action | consumer/application idempotency |

Transport-level protocol retry inside a Kafka client is not business retry policy.

## 7. Processing boundary

Event Stream does not implement:

```text
event -> transform
event -> enrich
event -> aggregate
event -> business decision
event -> workflow
event -> notification
event -> business retry
```

These belong to consumers or processing platforms.

Flink may consume Kafka events when stream processing is required. Event Stream does not reproduce Flink capabilities.

## 8. Deduplication

`Event.ID` remains stable across redelivery.

That is the transport contribution to deduplication. Whether duplicate processing is safe is business-specific and remains outside Event Stream.

## 9. Embedded versus Kafka

Use the embedded stream when process-local asynchronous fan-out and bounded burst absorption are sufficient.

Use Kafka when broker durability, external consumers, independent scaling or customer architecture requires it.

The event contract remains unchanged.

## 10. Boundary rule

Before adding a feature to Event Stream ask:

> Does this move an event, bound transport resources, prove transport acceptance, or expose transport state?

If not, it probably belongs elsewhere.
