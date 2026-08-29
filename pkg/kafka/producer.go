package kafka

import (
	"context"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

type Producer interface {
	Publish(ctx context.Context, topic string, key []byte, payload []byte) error
	Close() error
}

type KafkaProducer struct {
	writer *kafkago.Writer
}

func NewProducer(brokers []string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Balancer:     &kafkago.Hash{},
			RequiredAcks: kafkago.RequireAll,
			Async:        false,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, topic string, key []byte, payload []byte) error {
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   key,
		Value: payload,
	})
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

var _ Producer = (*KafkaProducer)(nil)
