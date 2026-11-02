package embedded

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
)

const (
	DefaultIngressCapacity      = 256
	DefaultSubscriptionCapacity = 64
	DefaultMaxSubscriptions     = 64
	DefaultMaxPayloadBytes      = 1 << 20
)

type Config struct {
	IngressCapacity      int
	SubscriptionCapacity int
	MaxSubscriptions     int
	MaxPayloadBytes      int
}

type counters struct {
	published     atomic.Uint64
	publishFailed atomic.Uint64
	read          atomic.Uint64
	committed     atomic.Uint64
	backpressure  atomic.Uint64
	inFlight      atomic.Int64
}

type Stream struct {
	cfg Config

	ingress chan eventstream.Event
	stop    chan struct{}
	stopped chan struct{}

	mu        sync.RWMutex
	accepting bool
	subs      map[string]*Subscription
	stopOnce  sync.Once
	stats     counters
}

type Subscription struct {
	name   string
	stream *Stream
	events chan eventstream.Event
	done   chan struct{}
	once   sync.Once
}

func New(cfg Config) (*Stream, error) {
	cfg = withDefaults(cfg)
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	s := &Stream{
		cfg:       cfg,
		ingress:   make(chan eventstream.Event, cfg.IngressCapacity),
		stop:      make(chan struct{}),
		stopped:   make(chan struct{}),
		accepting: true,
		subs:      make(map[string]*Subscription),
	}
	go s.dispatch()
	return s, nil
}

func withDefaults(cfg Config) Config {
	if cfg.IngressCapacity == 0 {
		cfg.IngressCapacity = DefaultIngressCapacity
	}
	if cfg.SubscriptionCapacity == 0 {
		cfg.SubscriptionCapacity = DefaultSubscriptionCapacity
	}
	if cfg.MaxSubscriptions == 0 {
		cfg.MaxSubscriptions = DefaultMaxSubscriptions
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	return cfg
}

func validateConfig(cfg Config) error {
	if cfg.IngressCapacity < 1 || cfg.SubscriptionCapacity < 1 || cfg.MaxSubscriptions < 1 || cfg.MaxPayloadBytes < 1 {
		return fmt.Errorf("embedded: capacities and payload limit must be positive")
	}
	return nil
}

func (s *Stream) Publish(ctx context.Context, event eventstream.Event) (eventstream.Receipt, error) {
	var zero eventstream.Receipt
	if err := event.Validate(); err != nil {
		s.stats.publishFailed.Add(1)
		return zero, err
	}
	if err := event.ValidatePayloadLimit(s.cfg.MaxPayloadBytes); err != nil {
		s.stats.publishFailed.Add(1)
		return zero, err
	}
	if err := ctx.Err(); err != nil {
		s.stats.publishFailed.Add(1)
		return zero, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.accepting {
		s.stats.publishFailed.Add(1)
		return zero, eventstream.ErrClosed
	}

	s.stats.inFlight.Add(1)
	select {
	case <-ctx.Done():
		s.stats.inFlight.Add(-1)
		s.stats.publishFailed.Add(1)
		return zero, ctx.Err()
	case s.ingress <- event:
		s.stats.published.Add(1)
		return eventstream.Receipt{
			EventID:     event.ID,
			Transport:   eventstream.TransportEmbedded,
			Destination: "embedded",
			Partition:   eventstream.NoPartition,
			Offset:      eventstream.NoOffset,
		}, nil
	default:
		s.stats.inFlight.Add(-1)
		s.stats.publishFailed.Add(1)
		s.stats.backpressure.Add(1)
		return zero, eventstream.ErrBackpressure
	}
}

func (s *Stream) Subscribe(name string, capacity int) (*Subscription, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, fmt.Errorf("embedded: subscription name is required")
	}
	if capacity == 0 {
		capacity = s.cfg.SubscriptionCapacity
	}
	if capacity < 1 {
		return nil, fmt.Errorf("embedded: subscription capacity must be positive")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.accepting {
		return nil, eventstream.ErrClosed
	}
	if _, exists := s.subs[name]; exists {
		return nil, fmt.Errorf("embedded: subscription %q already exists", name)
	}
	if len(s.subs) >= s.cfg.MaxSubscriptions {
		return nil, fmt.Errorf("embedded: maximum subscriptions reached")
	}

	sub := &Subscription{
		name:   name,
		stream: s,
		events: make(chan eventstream.Event, capacity),
		done:   make(chan struct{}),
	}
	s.subs[name] = sub
	return sub, nil
}

func (s *Stream) dispatch() {
	defer close(s.stopped)
	for {
		select {
		case <-s.stop:
			return
		case event := <-s.ingress:
			for _, sub := range s.snapshotSubscriptions() {
				select {
				case <-s.stop:
					return
				case <-sub.done:
					continue
				case sub.events <- event:
				}
			}
			s.stats.inFlight.Add(-1)
		}
	}
}

func (s *Stream) snapshotSubscriptions() []*Subscription {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Subscription, 0, len(s.subs))
	for _, sub := range s.subs {
		out = append(out, sub)
	}
	return out
}

func (s *Stream) Close(ctx context.Context) error {
	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		select {
		case <-s.stopped:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.accepting = false
	s.mu.Unlock()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for s.stats.inFlight.Load() > 0 {
		select {
		case <-ctx.Done():
			s.forceStop()
			return ctx.Err()
		case <-ticker.C:
		}
	}

	s.forceStop()
	select {
	case <-s.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Stream) forceStop() {
	s.stopOnce.Do(func() {
		close(s.stop)
		s.mu.Lock()
		subs := make([]*Subscription, 0, len(s.subs))
		for _, sub := range s.subs {
			subs = append(subs, sub)
		}
		s.subs = make(map[string]*Subscription)
		s.mu.Unlock()
		for _, sub := range subs {
			sub.once.Do(func() { close(sub.done) })
		}
	})
}

func (s *Stream) Stats() eventstream.Stats {
	return eventstream.Stats{
		Published:     s.stats.published.Load(),
		PublishFailed: s.stats.publishFailed.Load(),
		Read:          s.stats.read.Load(),
		Committed:     s.stats.committed.Load(),
		Backpressure:  s.stats.backpressure.Load(),
		InFlight:      s.stats.inFlight.Load(),
	}
}

func (s *Subscription) Read(ctx context.Context) (eventstream.Delivery, error) {
	var zero eventstream.Delivery
	makeDelivery := func(event eventstream.Event) eventstream.Delivery {
		s.stream.stats.read.Add(1)
		return eventstream.Delivery{
			Event:     event,
			Transport: eventstream.TransportEmbedded,
			Position: eventstream.Position{
				Destination: s.name,
				Partition:   eventstream.NoPartition,
				Offset:      eventstream.NoOffset,
			},
		}
	}

	select {
	case event := <-s.events:
		return makeDelivery(event), nil
	default:
	}

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case event := <-s.events:
		return makeDelivery(event), nil
	case <-s.done:
		return zero, eventstream.ErrClosed
	}
}

func (s *Subscription) Commit(_ context.Context, delivery eventstream.Delivery) error {
	if delivery.Transport != eventstream.TransportEmbedded {
		return fmt.Errorf("embedded: delivery transport %q does not belong to embedded stream", delivery.Transport)
	}
	s.stream.stats.committed.Add(1)
	return nil
}

func (s *Subscription) Close() error {
	s.once.Do(func() {
		close(s.done)
		s.stream.mu.Lock()
		delete(s.stream.subs, s.name)
		s.stream.mu.Unlock()
	})
	return nil
}

func IsBackpressure(err error) bool {
	return errors.Is(err, eventstream.ErrBackpressure)
}
