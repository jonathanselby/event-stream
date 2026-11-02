package eventstream

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidEvent = errors.New("invalid event")
	ErrOversize     = errors.New("event payload exceeds configured limit")
)

// Event is the transport-neutral application event envelope.
type Event struct {
	ID            string    `json:"id"`
	Type          string    `json:"type"`
	Version       string    `json:"version"`
	Source        string    `json:"source"`
	Subject       string    `json:"subject,omitempty"`
	OccurredAt    time.Time `json:"occurred_at"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	CausationID   string    `json:"causation_id,omitempty"`
	ContentType   string    `json:"content_type"`
	Schema        string    `json:"schema,omitempty"`
	Key           string    `json:"key,omitempty"`
	Payload       []byte    `json:"payload,omitempty"`
}

// Validate checks only the transport envelope. Payload business meaning is opaque.
func (e Event) Validate() error {
	checks := []struct {
		name  string
		value string
	}{
		{"id", e.ID},
		{"type", e.Type},
		{"version", e.Version},
		{"source", e.Source},
		{"content_type", e.ContentType},
	}
	for _, check := range checks {
		if strings.TrimSpace(check.value) == "" {
			return fmt.Errorf("%w: %s is required", ErrInvalidEvent, check.name)
		}
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is required", ErrInvalidEvent)
	}
	return nil
}

// ValidatePayloadLimit rejects payloads above a configured transport budget.
func (e Event) ValidatePayloadLimit(maxBytes int) error {
	if maxBytes <= 0 {
		return fmt.Errorf("%w: payload limit must be positive", ErrInvalidEvent)
	}
	if len(e.Payload) > maxBytes {
		return fmt.Errorf("%w: %d > %d bytes", ErrOversize, len(e.Payload), maxBytes)
	}
	return nil
}
