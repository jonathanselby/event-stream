package kafka

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testBroker(t *testing.T) string {
	t.Helper()
	broker := strings.TrimSpace(os.Getenv("KAFKA_BROKER"))
	if broker == "" {
		t.Skip("KAFKA_BROKER not configured; Kafka integration is exercised in CI")
	}
	return broker
}

func testTopic(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(t.Name())
	name = strings.NewReplacer("/", "-", "_", "-", " ", "-").Replace(name)
	return "event-stream-" + name
}

func independentProducer(t *testing.T, broker string) *kgo.Client {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func TestIndependentConsumerReadsEvent(t *testing.T) {
	broker := testBroker(t)
	topic := testTopic(t)
	publisher, err := NewPublisher(PublisherConfig{
		Brokers: []string{broker},
		Routes:  []Route{{Type: "case.approved", Version: "1", Topic: topic}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close(context.Background()) })

	want := kafkaTestEvent()
	receipt, err := publisher.Publish(context.Background(), want)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Destination != topic || receipt.Partition < 0 || receipt.Offset < 0 {
		t.Fatalf("unexpected receipt: %#v", receipt)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fetches := client.PollRecords(ctx, 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatal(errs[0].Err)
	}
	records := fetches.Records()
	if len(records) != 1 {
		t.Fatalf("expected one record, got %d", len(records))
	}
	got, err := decodeRecord(records[0])
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID {
		t.Fatalf("got event %q", got.ID)
	}
}

func TestReaderConsumesIndependentEvent(t *testing.T) {
	broker := testBroker(t)
	topic := testTopic(t)
	direct := independentProducer(t, broker)
	want := kafkaTestEvent()
	if err := direct.ProduceSync(context.Background(), encodeRecord(topic, want)).FirstErr(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(ReaderConfig{
		Brokers: []string{broker},
		Topics:  []string{topic},
		GroupID: "reader-independent-" + topic,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	delivery, err := reader.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Event.ID != want.ID {
		t.Fatalf("got event %q", delivery.Event.ID)
	}
}

func TestReadDoesNotImplicitlyCommit(t *testing.T) {
	broker := testBroker(t)
	topic := testTopic(t)
	direct := independentProducer(t, broker)
	if err := direct.ProduceSync(context.Background(), encodeRecord(topic, kafkaTestEvent())).FirstErr(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(ReaderConfig{Brokers: []string{broker}, Topics: []string{topic}, GroupID: "explicit-commit-" + topic})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := reader.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := reader.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Position != second.Position {
		t.Fatalf("reader advanced before commit: %#v then %#v", first.Position, second.Position)
	}
	if err := reader.Commit(ctx, first); err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
}

func TestKafkaPublishReceiptAndFailedPublish(t *testing.T) {
	broker := testBroker(t)
	topic := testTopic(t)
	publisher, err := NewPublisher(PublisherConfig{
		Brokers: []string{broker},
		Routes:  []Route{{Type: "case.approved", Version: "1", Topic: topic}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close(context.Background())

	receipt, err := publisher.Publish(context.Background(), kafkaTestEvent())
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Transport != eventstream.TransportKafka || receipt.Destination != topic || receipt.Partition < 0 || receipt.Offset < 0 {
		t.Fatalf("bad receipt: %#v", receipt)
	}

	bad := kafkaTestEvent()
	bad.Type = "missing"
	if receipt, err := publisher.Publish(context.Background(), bad); !errors.Is(err, ErrNoRoute) || receipt.EventID != "" {
		t.Fatalf("expected missing route and zero receipt, got %#v %v", receipt, err)
	}
}

func TestKafkaPartitionOrdering(t *testing.T) {
	broker := testBroker(t)
	topic := testTopic(t)
	publisher, err := NewPublisher(PublisherConfig{
		Brokers: []string{broker},
		Routes:  []Route{{Type: "ordered", Version: "1", Topic: topic}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer publisher.Close(context.Background())

	var last int64 = -1
	var partition int32 = -1
	for i := 0; i < 5; i++ {
		e := kafkaTestEvent()
		e.ID = fmt.Sprintf("evt-%d", i)
		e.Type = "ordered"
		e.Key = "same-key"
		receipt, err := publisher.Publish(context.Background(), e)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			partition = receipt.Partition
		} else if receipt.Partition != partition || receipt.Offset <= last {
			t.Fatalf("ordering changed: partition=%d offset=%d last=%d", receipt.Partition, receipt.Offset, last)
		}
		last = receipt.Offset
	}
}

func TestMalformedEnvelopeRefusedWithoutImplicitCommit(t *testing.T) {
	broker := testBroker(t)
	topic := testTopic(t)
	direct := independentProducer(t, broker)
	bad := &kgo.Record{
		Topic: topic,
		Key:   []byte("case-42"),
		Value: []byte(`{"approved":true}`),
		Headers: []kgo.RecordHeader{
			{Key: headerSpecVersion, Value: []byte("1.0")},
			{Key: headerID, Value: []byte("evt-bad")},
		},
	}
	if err := direct.ProduceSync(context.Background(), bad).FirstErr(); err != nil {
		t.Fatal(err)
	}

	reader, err := NewReader(ReaderConfig{Brokers: []string{broker}, Topics: []string{topic}, GroupID: "malformed-" + topic})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, err := reader.Read(ctx)
	if err == nil {
		t.Fatal("expected malformed envelope error")
	}
	if first.Position.Offset < 0 {
		t.Fatalf("expected positioned delivery, got %#v", first)
	}
	second, err := reader.Read(ctx)
	if err == nil || second.Position != first.Position {
		t.Fatalf("reader advanced malformed record before commit: first=%#v second=%#v err=%v", first.Position, second.Position, err)
	}
	if err := reader.Commit(ctx, first); err != nil {
		t.Fatal(err)
	}
}

func TestConsumerGroupOffsets(t *testing.T) {
	broker := testBroker(t)
	topic := testTopic(t)
	direct := independentProducer(t, broker)
	for i := 1; i <= 2; i++ {
		e := kafkaTestEvent()
		e.ID = fmt.Sprintf("evt-%d", i)
		if err := direct.ProduceSync(context.Background(), encodeRecord(topic, e)).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	cfg := ReaderConfig{Brokers: []string{broker}, Topics: []string{topic}, GroupID: "offset-restart-" + topic}
	reader, err := NewReader(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	first, err := reader.Read(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if first.Event.ID != "evt-1" {
		cancel()
		t.Fatalf("expected first event, got %q", first.Event.ID)
	}
	if err := reader.Commit(ctx, first); err != nil {
		cancel()
		t.Fatal(err)
	}
	cancel()
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	reader2, err := NewReader(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer reader2.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	second, err := reader2.Read(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if second.Event.ID != "evt-2" {
		t.Fatalf("expected committed group to resume at second event, got %q", second.Event.ID)
	}
}
