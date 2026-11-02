# Event Stream — Release Checklist

**Implementation evidence commit:** `ba07d158c9789a105f2a63c746719d1d6683b493`  
**Release-gate candidate:** `7db5ce91f8ad843984cf7599864b51fd4758540c`

## Source/version

- [ ] `VERSION` and `eventstream.Version` agree.
- [ ] changelog contains the release version.
- [ ] release tag is `v<VERSION>`.

## Contract gate

- [ ] event envelope validation passes.
- [ ] embedded stream is bounded and non-durable.
- [ ] accepted embedded events are not silently discarded during normal operation.
- [ ] exact Kafka route resolution passes.
- [ ] Kafka receipt contains acknowledged topic/partition/offset.
- [ ] independent producer and consumer interoperability passes.
- [ ] reader does not commit before explicit caller commit.
- [ ] no business retry, workflow, transformation or deduplication store exists in the component.

## Engineering gate

- [ ] `gofmt -l .` is empty.
- [ ] `go vet ./...` passes.
- [ ] `go test -race -count=1 ./...` passes.
- [ ] examples build.
- [ ] `linux/arm64` package build passes.

## Evidence

- [ ] every PRD acceptance criterion maps to executable evidence.
- [ ] `generated/EDD.md` is pinned to the implementation evidence commit.
- [ ] `generated/COMPARISON.md` contains no unexplained mismatch.
