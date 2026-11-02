package main

import (
	"context"
	"fmt"
	"os"

	eskafka "github.com/jonathanselby/event-stream/kafka"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "127.0.0.1:9092"
	}
	reader, err := eskafka.NewReader(eskafka.ReaderConfig{Brokers: []string{broker}, Topics: []string{"case-events"}, GroupID: "event-stream-example"})
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	delivery, err := reader.Read(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Printf("event=%s topic=%s partition=%d offset=%d\n", delivery.Event.ID, delivery.Position.Destination, delivery.Position.Partition, delivery.Position.Offset)
	if err := reader.Commit(context.Background(), delivery); err != nil {
		panic(err)
	}
}
