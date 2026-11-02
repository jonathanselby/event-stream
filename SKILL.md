# Event Stream — Skill

## Use this when

Changing the Event Stream contract, embedded transport, Kafka mapping, routing, receipts, reader commit semantics or transport bounds.

## Core rule

> **Move events. Do not process them.**

## Change discipline

1. Keep `Event` transport-neutral. Kafka topics and credentials never enter it.
2. Keep embedded memory, payloads, subscriptions and Kafka buffering/fetching explicitly bounded.
3. Do not silently discard accepted embedded events to relieve pressure.
4. Preserve exact type/version topic routing; do not add a routing language without a demonstrated need.
5. A Kafka receipt proves broker acknowledgement only.
6. `Read` never implies commit. Do not advance past an outstanding delivery.
7. Keep Kafka semantics at least once; business idempotency remains outside.
8. Do not add workflow, transformation, business retry, dead-letter policy, deduplication storage, SQL persistence or Kafka administration.
9. Keep `PROTOCOL.md` synchronized with the actual wire codec.
10. Run format, vet, race, interoperability, version and ARM64 gates before release.
11. Regenerate implementation-derived evidence and compare it with the hand-written design before tagging.
