package main

import (
	"context"
	"fmt"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
	"github.com/jonathanselby/event-stream/embedded"
)

func main() {
	stream, err := embedded.New(embedded.Config{})
	if err != nil {
		panic(err)
	}
	defer stream.Close(context.Background())

	sub, err := stream.Subscribe("example", 16)
	if err != nil {
		panic(err)
	}
	defer sub.Close()

	event := eventstream.Event{ID: "evt-example", Type: "person.safe", Version: "1", Source: "example", OccurredAt: time.Now().UTC(), ContentType: "application/json", Payload: []byte(`{"safe":true}`)}
	if _, err := stream.Publish(context.Background(), event); err != nil {
		panic(err)
	}
	delivery, err := sub.Read(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(delivery.Event.ID)
}
