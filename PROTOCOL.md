# Event Stream — Kafka Protocol Mapping

**Version:** 0.1

## 1. Purpose

Define the stable Kafka record mapping used by Event Stream V0.1.

The mapping follows CloudEvents Kafka binary-mode conventions for core attributes while retaining the component's explicit event version, correlation and causation fields.

## 2. Record mapping

| Event field | Kafka location | Header/key |
|---|---|---|
| `Key` | record key | — |
| `Payload` | record value | — |
| CloudEvents version | header | `ce_specversion=1.0` |
| `ID` | header | `ce_id` |
| `Type` | header | `ce_type` |
| `Source` | header | `ce_source` |
| `Subject` | header | `ce_subject` |
| `OccurredAt` | header | `ce_time` RFC3339Nano UTC |
| `ContentType` | header | `content-type` |
| `Schema` | header | `ce_dataschema` |
| `Version` | header | `event-version` |
| `CorrelationID` | header | `correlation-id` |
| `CausationID` | header | `causation-id` |

Optional headers are omitted when their source field is empty.

The event payload is never wrapped or transformed by the Kafka adapter.

## 3. Topic routing

Topic is not part of the Event.

Exact `(Type, Version)` configuration resolves the destination topic before publication. Missing routes are refused.

## 4. Receipt

Broker acknowledgement returns:

```text
event_id
topic
partition
offset
```

A receipt is transport evidence only.

## 5. Reader behaviour

Reader reconstructs the Event from the record key, value and headers.

The reader requires `ce_specversion=1.0`. Duplicate header names, invalid time values or missing required envelope fields produce a positioned delivery plus an error. No offset is committed implicitly.

## 6. Compatibility boundary

V0.1 supports Kafka-compatible brokers through `franz-go`.

CloudEvents structured mode, schema-registry serialization, content transformation, topic administration and customer-specific headers are outside V0.1.
