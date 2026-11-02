package main

import (
	"context"
	"fmt"
	"os"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
	eskafka "github.com/jonathanselby/event-stream/kafka"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "127.0.0.1:9092"
	}
	publisher, err := eskafka.NewPublisher(eskafka.PublisherConfig{Brokers: []string{broker}, Routes: []eskafka.Route{{Type: "case.approved", Version: "1", Topic: "case-events"}}})
	if err != nil {
		panic(err)
	}
	defer publisher.Close(context.Background())

	receipt, err := publisher.Publish(context.Background(), eventstream.Event{ID: "evt-example", Type: "case.approved", Version: "1", Source: "example", OccurredAt: time.Now().UTC(), ContentType: "application/json", Key: "case-42", Payload: []byte(`{"case_id":"42"}`)})
	if err != nil {
		panic(err)
	}
	fmt.Printf("topic=%s partition=%d offset=%d\n", receipt.Destination, receipt.Partition, receipt.Offset)
}
