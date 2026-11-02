package kafka

import (
	"fmt"
	"time"

	eventstream "github.com/jonathanselby/event-stream"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	headerSpecVersion   = "ce_specversion"
	headerID            = "ce_id"
	headerType          = "ce_type"
	headerSource        = "ce_source"
	headerSubject       = "ce_subject"
	headerTime          = "ce_time"
	headerDataSchema    = "ce_dataschema"
	headerContentType   = "content-type"
	headerEventVersion  = "event-version"
	headerCorrelationID = "correlation-id"
	headerCausationID   = "causation-id"
)

func encodeRecord(topic string, event eventstream.Event) *kgo.Record {
	headers := []kgo.RecordHeader{
		{Key: headerSpecVersion, Value: []byte("1.0")},
		{Key: headerID, Value: []byte(event.ID)},
		{Key: headerType, Value: []byte(event.Type)},
		{Key: headerSource, Value: []byte(event.Source)},
		{Key: headerTime, Value: []byte(event.OccurredAt.UTC().Format(time.RFC3339Nano))},
		{Key: headerContentType, Value: []byte(event.ContentType)},
		{Key: headerEventVersion, Value: []byte(event.Version)},
	}
	optional := []struct {
		key   string
		value string
	}{
		{headerSubject, event.Subject},
		{headerDataSchema, event.Schema},
		{headerCorrelationID, event.CorrelationID},
		{headerCausationID, event.CausationID},
	}
	for _, item := range optional {
		if item.value != "" {
			headers = append(headers, kgo.RecordHeader{Key: item.key, Value: []byte(item.value)})
		}
	}

	return &kgo.Record{
		Topic:   topic,
		Key:     []byte(event.Key),
		Value:   append([]byte(nil), event.Payload...),
		Headers: headers,
	}
}

func decodeRecord(record *kgo.Record) (eventstream.Event, error) {
	values := make(map[string]string, len(record.Headers))
	for _, header := range record.Headers {
		if _, exists := values[header.Key]; exists {
			return eventstream.Event{Key: string(record.Key), Payload: append([]byte(nil), record.Value...)}, fmt.Errorf("duplicate Kafka header %q", header.Key)
		}
		values[header.Key] = string(header.Value)
	}

	event := eventstream.Event{
		ID:            values[headerID],
		Type:          values[headerType],
		Version:       values[headerEventVersion],
		Source:        values[headerSource],
		Subject:       values[headerSubject],
		CorrelationID: values[headerCorrelationID],
		CausationID:   values[headerCausationID],
		ContentType:   values[headerContentType],
		Schema:        values[headerDataSchema],
		Key:           string(record.Key),
		Payload:       append([]byte(nil), record.Value...),
	}

	if values[headerSpecVersion] != "1.0" {
		return event, fmt.Errorf("unsupported ce_specversion %q", values[headerSpecVersion])
	}
	if raw := values[headerTime]; raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			return event, fmt.Errorf("invalid ce_time: %w", err)
		}
		event.OccurredAt = parsed
	}
	if err := event.Validate(); err != nil {
		return event, err
	}
	return event, nil
}
