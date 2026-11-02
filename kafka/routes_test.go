package kafka

import (
	"errors"
	"testing"
)

func TestKafkaRouteResolution(t *testing.T) {
	router, err := NewRouter([]Route{{Type: "case.approved", Version: "1", Topic: "case-events"}})
	if err != nil {
		t.Fatal(err)
	}
	topic, err := router.Resolve("case.approved", "1")
	if err != nil {
		t.Fatal(err)
	}
	if topic != "case-events" {
		t.Fatalf("got %q", topic)
	}
}

func TestMissingRouteRejected(t *testing.T) {
	router, err := NewRouter([]Route{{Type: "case.approved", Version: "1", Topic: "case-events"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Resolve("case.rejected", "1"); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("expected no route, got %v", err)
	}
}

func TestDuplicateRouteRejected(t *testing.T) {
	_, err := NewRouter([]Route{
		{Type: "case.approved", Version: "1", Topic: "a"},
		{Type: "case.approved", Version: "1", Topic: "b"},
	})
	if err == nil {
		t.Fatal("expected duplicate route refusal")
	}
}
