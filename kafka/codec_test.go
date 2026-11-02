package kafka

import (
	"testing"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
)

func kafkaTestEvent() eventstream.Event {
	return eventstream.Event{
		ID:            "evt-1",
		Type:          "case.approved",
		Version:       "1",
		Source:        "case-service",
		Subject:       "case-42",
		OccurredAt:    time.Unix(1_700_000_000, 123).UTC(),
		CorrelationID: "corr-1",
		CausationID:   "cause-1",
		ContentType:   "application/json",
		Schema:        "case-approved-v1",
		Key:           "case-42",
		Payload:       []byte(`{"approved":true}`),
	}
}

func TestKafkaCodecRoundTrip(t *testing.T) {
	want := kafkaTestEvent()
	record := encodeRecord("case-events", want)
	got, err := decodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID || got.Type != want.Type || got.Version != want.Version || got.Key != want.Key || string(got.Payload) != string(want.Payload) {
		t.Fatalf("round trip mismatch: %#v", got)
	}
}
