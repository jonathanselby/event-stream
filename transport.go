package eventstream

import (
	"context"
	"errors"
)

var (
	ErrBackpressure = errors.New("event stream back-pressure")
	ErrClosed       = errors.New("event stream closed")
)

const (
	TransportEmbedded       = "embedded"
	TransportKafka          = "kafka"
	NoPartition       int32 = -1
	NoOffset          int64 = -1
)

// Receipt is evidence that a transport accepted a published event.
type Receipt struct {
	EventID     string `json:"event_id"`
	Transport   string `json:"transport"`
	Destination string `json:"destination"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
}

// Position identifies a transport position for a delivered event.
type Position struct {
	Destination string `json:"destination"`
	Partition   int32  `json:"partition"`
	Offset      int64  `json:"offset"`
	LeaderEpoch int32  `json:"leader_epoch,omitempty"`
}

// Delivery is an event plus the transport position from which it was read.
// If Read returns both a Delivery and an error, Position may still be committed
// explicitly by the caller to skip a malformed transport envelope.
type Delivery struct {
	Event     Event    `json:"event"`
	Transport string   `json:"transport"`
	Position  Position `json:"position"`
}

type Publisher interface {
	Publish(context.Context, Event) (Receipt, error)
}

type Reader interface {
	Read(context.Context) (Delivery, error)
	Commit(context.Context, Delivery) error
	Close() error
}

// Stats exposes transport outcomes without embedding an observability platform.
type Stats struct {
	Published     uint64 `json:"published"`
	PublishFailed uint64 `json:"publish_failed"`
	Read          uint64 `json:"read"`
	Committed     uint64 `json:"committed"`
	Backpressure  uint64 `json:"backpressure"`
	InFlight      int64  `json:"in_flight"`
}
