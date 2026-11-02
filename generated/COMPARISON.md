# Event Stream — Design / Implementation Comparison

**Implementation evidence commit:** `{{EVIDENCE_COMMIT}}`  
**Generated:** 2026-11-02

| Design area | Hand-written intent | Recovered implementation | Assessment |
|---|---|---|---|
| component boundary | transport only | core + embedded + Kafka transport packages; no processing platform | MATCH |
| event model | stable transport-neutral envelope | `eventstream.Event` contains no Kafka topic/security fields | MATCH |
| embedded stream | bounded asynchronous fan-out | bounded ingress + bounded named subscriptions + one dispatcher | MATCH |
| back-pressure | explicit, no silent eviction | full ingress returns `ErrBackpressure`; slow subscriber propagates pressure | MATCH |
| embedded durability | explicitly non-durable | memory-only channels; no store | MATCH |
| resource budgets | explicit payload/buffer/subscription bounds | configurable limits with finite defaults | MATCH |
| Kafka routing | exact external type/version mapping | validated `kafka.Router` | MATCH |
| Kafka wire mapping | payload unchanged; metadata in headers | binary-style CloudEvents headers + explicit extensions | MATCH |
| Kafka receipt | broker acceptance only | success returns topic/partition/offset | MATCH |
| Kafka reader | explicit commit, at least once | auto-commit disabled; one outstanding positioned delivery | MATCH |
| partition ordering | no second scheduler | one outstanding reader record; Kafka partitions provide ordering/parallelism | MATCH |
| malformed records | visible, no implicit commit | positioned delivery plus decode error remains outstanding | MATCH |
| retry boundary | protocol retry only | franz-go bounded record retries; no business retry engine | MATCH |
| processing boundary | no Flink/workflow/transformation | no corresponding runtime packages or dependencies | MATCH |
| persistence boundary | no SQL/outbox implementation | no database dependency or persistence package | MATCH |
| security boundary | connection config external | Kafka client options supplied externally; no credentials in Event/Receipt | MATCH |
| observability | small transport statistics | `Stats` counters only | MATCH |
| constrained build | Go 1.26 + ARM64 proof | CI contains native/race and Linux ARM64 build gates | MATCH |

## Corrections made before release

- made embedded publication/close synchronization explicit so shutdown cannot race a new accepted publish;
- fixed embedded in-flight accounting so dispatch cannot decrement before publication increments;
- preserved already-buffered subscription events across graceful stream closure;
- added explicit maximum subscription and payload budgets;
- made Kafka malformed-envelope handling positioned and caller-controlled rather than implicitly committing or inventing dead-letter policy;
- added consumer-group restart evidence to prove committed offset behaviour.

## Result

MATCH after correction. No unexplained design/implementation mismatch remains at the evidence commit.
