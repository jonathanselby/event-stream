package eventstream

import (
	"errors"
	"testing"
	"time"
)

func validEvent() Event {
	return Event{
		ID:          "evt-1",
		Type:        "person.safe",
		Version:     "1",
		Source:      "safety",
		OccurredAt:  time.Unix(1_700_000_000, 0).UTC(),
		ContentType: "application/json",
		Payload:     []byte(`{"safe":true}`),
	}
}

func TestEventValidate(t *testing.T) {
	e := validEvent()
	if err := e.Validate(); err != nil {
		t.Fatalf("valid event refused: %v", err)
	}

	e.ID = ""
	if err := e.Validate(); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("expected invalid event, got %v", err)
	}
}

func TestEventPayloadLimit(t *testing.T) {
	e := validEvent()
	if err := e.ValidatePayloadLimit(len(e.Payload)); err != nil {
		t.Fatalf("exact limit refused: %v", err)
	}
	if err := e.ValidatePayloadLimit(len(e.Payload) - 1); !errors.Is(err, ErrOversize) {
		t.Fatalf("expected oversize, got %v", err)
	}
}
