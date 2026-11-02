package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrNoRoute = errors.New("no Kafka route configured")

const (
	DefaultMaxPayloadBytes   = 1 << 20
	DefaultMaxBufferedBytes  = 8 << 20
	DefaultMaxBufferedRecord = 256
	DefaultDeliveryTimeout   = 10 * time.Second
	DefaultRecordRetries     = 3
	DefaultFetchMaxBytes     = 8 << 20
	DefaultFetchPartition    = 1 << 20
)

type PublisherConfig struct {
	Brokers            []string
	Routes             []Route
	MaxPayloadBytes    int
	MaxBufferedBytes   int
	MaxBufferedRecords int
	DeliveryTimeout    time.Duration
	RecordRetries      int
	ClientOptions      []kgo.Opt
}

type ReaderConfig struct {
	Brokers                []string
	Topics                 []string
	GroupID                string
	MaxPayloadBytes        int
	FetchMaxBytes          int32
	FetchMaxPartitionBytes int32
	ClientOptions          []kgo.Opt
}

type kafkaCounters struct {
	published     atomic.Uint64
	publishFailed atomic.Uint64
	read          atomic.Uint64
	committed     atomic.Uint64
	inFlight      atomic.Int64
}

type Publisher struct {
	client          *kgo.Client
	router          Router
	maxPayloadBytes int
	stats           kafkaCounters
}

type Reader struct {
	client          *kgo.Client
	maxPayloadBytes int
	stats           kafkaCounters

	mu             sync.Mutex
	outstanding    *eventstream.Delivery
	outstandingErr error
	closed         bool
}

func NewPublisher(cfg PublisherConfig) (*Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: at least one broker is required")
	}
	router, err := NewRouter(cfg.Routes)
	if err != nil {
		return nil, err
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if cfg.MaxBufferedBytes == 0 {
		cfg.MaxBufferedBytes = DefaultMaxBufferedBytes
	}
	if cfg.MaxBufferedRecords == 0 {
		cfg.MaxBufferedRecords = DefaultMaxBufferedRecord
	}
	if cfg.DeliveryTimeout == 0 {
		cfg.DeliveryTimeout = DefaultDeliveryTimeout
	}
	if cfg.RecordRetries == 0 {
		cfg.RecordRetries = DefaultRecordRetries
	}
	if cfg.MaxPayloadBytes < 1 || cfg.MaxBufferedBytes < 1 || cfg.MaxBufferedRecords < 1 || cfg.DeliveryTimeout <= 0 || cfg.RecordRetries < 0 {
		return nil, fmt.Errorf("kafka: invalid producer bounds")
	}

	opts := append([]kgo.Opt{}, cfg.ClientOptions...)
	opts = append(opts,
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxBufferedBytes(cfg.MaxBufferedBytes),
		kgo.MaxBufferedRecords(cfg.MaxBufferedRecords),
		kgo.RecordDeliveryTimeout(cfg.DeliveryTimeout),
		kgo.RecordRetries(cfg.RecordRetries),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Publisher{client: client, router: router, maxPayloadBytes: cfg.MaxPayloadBytes}, nil
}

func (p *Publisher) Publish(ctx context.Context, event eventstream.Event) (eventstream.Receipt, error) {
	var zero eventstream.Receipt
	if err := event.Validate(); err != nil {
		p.stats.publishFailed.Add(1)
		return zero, err
	}
	if err := event.ValidatePayloadLimit(p.maxPayloadBytes); err != nil {
		p.stats.publishFailed.Add(1)
		return zero, err
	}
	topic, err := p.router.Resolve(event.Type, event.Version)
	if err != nil {
		p.stats.publishFailed.Add(1)
		return zero, err
	}

	p.stats.inFlight.Add(1)
	defer p.stats.inFlight.Add(-1)
	record, err := p.client.ProduceSync(ctx, encodeRecord(topic, event)).First()
	if err != nil {
		p.stats.publishFailed.Add(1)
		return zero, err
	}
	p.stats.published.Add(1)
	return eventstream.Receipt{
		EventID:     event.ID,
		Transport:   eventstream.TransportKafka,
		Destination: record.Topic,
		Partition:   record.Partition,
		Offset:      record.Offset,
	}, nil
}

func (p *Publisher) Stats() eventstream.Stats {
	return eventstream.Stats{
		Published:     p.stats.published.Load(),
		PublishFailed: p.stats.publishFailed.Load(),
		InFlight:      p.stats.inFlight.Load(),
	}
}

func (p *Publisher) Close(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		_ = p.client.AbortBufferedRecords(ctx)
		p.client.Close()
		return err
	}
	p.client.Close()
	return nil
}

func NewReader(cfg ReaderConfig) (*Reader, error) {
	if len(cfg.Brokers) == 0 || len(cfg.Topics) == 0 || cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka: brokers, topics and group ID are required")
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = DefaultMaxPayloadBytes
	}
	if cfg.FetchMaxBytes == 0 {
		cfg.FetchMaxBytes = DefaultFetchMaxBytes
	}
	if cfg.FetchMaxPartitionBytes == 0 {
		cfg.FetchMaxPartitionBytes = DefaultFetchPartition
	}
	if cfg.MaxPayloadBytes < 1 || cfg.FetchMaxBytes < 1 || cfg.FetchMaxPartitionBytes < 1 {
		return nil, fmt.Errorf("kafka: invalid reader bounds")
	}

	opts := append([]kgo.Opt{}, cfg.ClientOptions...)
	opts = append(opts,
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.MaxConcurrentFetches(1),
		kgo.FetchMaxBytes(cfg.FetchMaxBytes),
		kgo.FetchMaxPartitionBytes(cfg.FetchMaxPartitionBytes),
	)
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Reader{client: client, maxPayloadBytes: cfg.MaxPayloadBytes}, nil
}

func (r *Reader) Read(ctx context.Context) (eventstream.Delivery, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return eventstream.Delivery{}, eventstream.ErrClosed
	}
	if r.outstanding != nil {
		return *r.outstanding, r.outstandingErr
	}

	fetches := r.client.PollRecords(ctx, 1)
	if errs := fetches.Errors(); len(errs) > 0 {
		return eventstream.Delivery{}, errs[0].Err
	}
	records := fetches.Records()
	if len(records) == 0 {
		if err := ctx.Err(); err != nil {
			return eventstream.Delivery{}, err
		}
		return eventstream.Delivery{}, fmt.Errorf("kafka: poll returned no record")
	}
	record := records[0]
	event, decodeErr := decodeRecord(record)
	delivery := eventstream.Delivery{
		Event:     event,
		Transport: eventstream.TransportKafka,
		Position: eventstream.Position{
			Destination: record.Topic,
			Partition:   record.Partition,
			Offset:      record.Offset,
			LeaderEpoch: record.LeaderEpoch,
		},
	}
	if decodeErr == nil {
		decodeErr = event.ValidatePayloadLimit(r.maxPayloadBytes)
	}
	r.outstanding = &delivery
	r.outstandingErr = decodeErr
	r.stats.read.Add(1)
	return delivery, decodeErr
}

func (r *Reader) Commit(ctx context.Context, delivery eventstream.Delivery) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return eventstream.ErrClosed
	}
	if r.outstanding == nil {
		return fmt.Errorf("kafka: no outstanding delivery")
	}
	want := r.outstanding.Position
	got := delivery.Position
	if delivery.Transport != eventstream.TransportKafka || want.Destination != got.Destination || want.Partition != got.Partition || want.Offset != got.Offset {
		return fmt.Errorf("kafka: commit does not match outstanding delivery")
	}

	record := &kgo.Record{
		Topic:       got.Destination,
		Partition:   got.Partition,
		Offset:      got.Offset,
		LeaderEpoch: got.LeaderEpoch,
	}
	if err := r.client.CommitRecords(ctx, record); err != nil {
		return err
	}
	r.client.AllowRebalance()
	r.outstanding = nil
	r.outstandingErr = nil
	r.stats.committed.Add(1)
	return nil
}

func (r *Reader) Stats() eventstream.Stats {
	return eventstream.Stats{Read: r.stats.read.Load(), Committed: r.stats.committed.Load()}
}

func (r *Reader) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.client.AllowRebalance()
	r.mu.Unlock()
	r.client.CloseAllowingRebalance()
	return nil
}
