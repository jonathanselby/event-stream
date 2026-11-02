package embedded

import (
	"context"
	"errors"
	"testing"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
)

func testEvent(id string) eventstream.Event {
	return eventstream.Event{
		ID:          id,
		Type:        "person.safe",
		Version:     "1",
		Source:      "safety",
		OccurredAt:  time.Unix(1_700_000_000, 0).UTC(),
		ContentType: "application/json",
		Payload:     []byte(`{"safe":true}`),
	}
}

func TestEmbeddedFanout(t *testing.T) {
	s, err := New(Config{IngressCapacity: 4, SubscriptionCapacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	a, err := s.Subscribe("a", 4)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Subscribe("b", 4)
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := s.Publish(context.Background(), testEvent("evt-1"))
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Transport != eventstream.TransportEmbedded || receipt.EventID != "evt-1" {
		t.Fatalf("unexpected receipt: %#v", receipt)
	}

	for _, sub := range []*Subscription{a, b} {
		d, err := sub.Read(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if d.Event.ID != "evt-1" {
			t.Fatalf("got %q", d.Event.ID)
		}
	}
}

func TestEmbeddedBackpressure(t *testing.T) {
	s, err := New(Config{IngressCapacity: 1, SubscriptionCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	sub, err := s.Subscribe("blocked", 1)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Publish(context.Background(), testEvent("1")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for s.Stats().InFlight != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := s.Publish(context.Background(), testEvent("2")); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(time.Second)
	for len(s.ingress) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(s.ingress) != 0 {
		t.Fatal("dispatcher did not reach blocked subscriber")
	}
	if _, err := s.Publish(context.Background(), testEvent("3")); err != nil {
		t.Fatal(err)
	}

	var gotBackpressure bool
	for i := 4; i < 100; i++ {
		_, err := s.Publish(context.Background(), testEvent(string(rune(i))))
		if errors.Is(err, eventstream.ErrBackpressure) {
			gotBackpressure = true
			break
		}
	}
	if !gotBackpressure {
		t.Fatal("expected explicit back-pressure")
	}

	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, readErr := sub.Read(ctx)
		cancel()
		if readErr != nil {
			t.Fatalf("drain: %v", readErr)
		}
	}
}

func TestEmbeddedAcceptedEventsNotDiscarded(t *testing.T) {
	s, err := New(Config{IngressCapacity: 8, SubscriptionCapacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := s.Subscribe("reader", 8)
	if err != nil {
		t.Fatal(err)
	}

	const count = 8
	for i := 0; i < count; i++ {
		if _, err := s.Publish(context.Background(), testEvent(string(rune('a'+i)))); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < count; i++ {
		if _, err := sub.Read(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddedMaxSubscriptions(t *testing.T) {
	s, err := New(Config{MaxSubscriptions: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())
	if _, err := s.Subscribe("one", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Subscribe("two", 1); err == nil {
		t.Fatal("expected maximum subscription refusal")
	}
}

func TestGracefulShutdownPreservesBufferedSubscriptionEvents(t *testing.T) {
	s, err := New(Config{IngressCapacity: 4, SubscriptionCapacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := s.Subscribe("reader", 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(context.Background(), testEvent("buffered")); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for s.Stats().InFlight > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.Stats().InFlight != 0 {
		t.Fatal("event did not reach subscription buffer")
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	delivery, err := sub.Read(context.Background())
	if err != nil {
		t.Fatalf("buffered event lost on graceful shutdown: %v", err)
	}
	if delivery.Event.ID != "buffered" {
		t.Fatalf("got %q", delivery.Event.ID)
	}
	if _, err := sub.Read(context.Background()); !errors.Is(err, eventstream.ErrClosed) {
		t.Fatalf("expected closed after buffer drained, got %v", err)
	}
}

func TestEmbeddedPayloadBound(t *testing.T) {
	s, err := New(Config{MaxPayloadBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	e := testEvent("oversize")
	e.Payload = []byte("12345")
	if _, err := s.Publish(context.Background(), e); !errors.Is(err, eventstream.ErrOversize) {
		t.Fatalf("expected oversize, got %v", err)
	}
}

func TestGracefulShutdown(t *testing.T) {
	s, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Publish(context.Background(), testEvent("late")); !errors.Is(err, eventstream.ErrClosed) {
		t.Fatalf("expected closed, got %v", err)
	}
}
