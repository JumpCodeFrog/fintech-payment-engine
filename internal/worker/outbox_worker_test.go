package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/JumpCodeFrog/fintech-payment-engine/internal/domain"
	"github.com/google/uuid"
)

func TestOutboxWorkerProcessBatch(t *testing.T) {
	publishErr := errors.New("kafka unavailable")

	tests := []struct {
		name            string
		event           *domain.OutboxEvent
		producerErr     error
		maxRetries      int
		wantStatus      domain.OutboxStatus
		wantRetryCount  int
		wantPublishedAt bool
	}{
		{
			name: "publishes event",
			event: &domain.OutboxEvent{
				ID:          uuid.New(),
				AggregateID: "payment-1",
				Payload:     []byte(`{"transaction_id":"payment-1"}`),
				Status:      domain.OutboxStatusPending,
			},
			maxRetries:      3,
			wantStatus:      domain.OutboxStatusPublished,
			wantPublishedAt: true,
		},
		{
			name: "keeps event pending before retry limit",
			event: &domain.OutboxEvent{
				ID:          uuid.New(),
				AggregateID: "payment-2",
				Payload:     []byte(`{"transaction_id":"payment-2"}`),
				Status:      domain.OutboxStatusPending,
				RetryCount:  1,
			},
			producerErr:    publishErr,
			maxRetries:     3,
			wantStatus:     domain.OutboxStatusPending,
			wantRetryCount: 2,
		},
		{
			name: "marks event failed at retry limit",
			event: &domain.OutboxEvent{
				ID:          uuid.New(),
				AggregateID: "payment-3",
				Payload:     []byte(`{"transaction_id":"payment-3"}`),
				Status:      domain.OutboxStatusPending,
				RetryCount:  2,
			},
			producerErr:    publishErr,
			maxRetries:     3,
			wantStatus:     domain.OutboxStatusFailed,
			wantRetryCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &outboxRepositoryStub{events: []*domain.OutboxEvent{tt.event}}
			producer := &producerStub{err: tt.producerErr}
			transactor := &transactorStub{}
			worker := NewOutboxWorker(
				repo,
				transactor,
				producer,
				slog.New(slog.NewTextHandler(io.Discard, nil)),
				WorkerConfig{
					BatchSize:    10,
					PollInterval: time.Second,
					MaxRetries:   tt.maxRetries,
					Topic:        "payments",
				},
			)

			if err := worker.processBatch(context.Background()); err != nil {
				t.Fatalf("processBatch() error = %v", err)
			}
			if transactor.calls != 1 {
				t.Fatalf("transactor calls = %d, want 1", transactor.calls)
			}
			if repo.limit != 10 {
				t.Errorf("GetPending limit = %d, want 10", repo.limit)
			}
			if len(producer.messages) != 1 {
				t.Fatalf("published messages = %d, want 1", len(producer.messages))
			}
			message := producer.messages[0]
			if message.topic != "payments" {
				t.Errorf("topic = %q, want payments", message.topic)
			}
			if string(message.key) != tt.event.AggregateID {
				t.Errorf("key = %q, want %q", message.key, tt.event.AggregateID)
			}
			if string(message.payload) != string(tt.event.Payload) {
				t.Errorf("payload = %q, want %q", message.payload, tt.event.Payload)
			}
			if len(repo.updated) != 1 {
				t.Fatalf("updated events = %d, want 1", len(repo.updated))
			}
			updated := repo.updated[0]
			if updated.Status != tt.wantStatus {
				t.Errorf("status = %s, want %s", updated.Status, tt.wantStatus)
			}
			if updated.RetryCount != tt.wantRetryCount {
				t.Errorf("retry count = %d, want %d", updated.RetryCount, tt.wantRetryCount)
			}
			if (updated.PublishedAt != nil) != tt.wantPublishedAt {
				t.Errorf("published at = %v, want presence %t", updated.PublishedAt, tt.wantPublishedAt)
			}
		})
	}
}

func TestOutboxWorkerStartStopsAfterCurrentBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo := &outboxRepositoryStub{events: []*domain.OutboxEvent{{
		ID:          uuid.New(),
		AggregateID: "payment-1",
		Payload:     []byte(`{}`),
		Status:      domain.OutboxStatusPending,
	}}}
	producer := &producerStub{publishStarted: make(chan struct{}), release: make(chan struct{})}
	worker := NewOutboxWorker(
		repo,
		&transactorStub{},
		producer,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WorkerConfig{BatchSize: 1, PollInterval: time.Millisecond, MaxRetries: 3, Topic: "payments"},
	)

	done := make(chan error, 1)
	go func() {
		done <- worker.Start(ctx)
	}()

	select {
	case <-producer.publishStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not start publishing")
	}
	cancel()

	select {
	case err := <-done:
		t.Fatalf("Start() returned before current batch completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(producer.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after current batch")
	}

	if len(repo.updated) != 1 || repo.updated[0].Status != domain.OutboxStatusPublished {
		t.Fatalf("current batch was not completed: %#v", repo.updated)
	}
}

type outboxRepositoryStub struct {
	events  []*domain.OutboxEvent
	updated []*domain.OutboxEvent
	limit   int
}

func (r *outboxRepositoryStub) Create(_ context.Context, _ *domain.OutboxEvent) error {
	return nil
}

func (r *outboxRepositoryStub) GetPending(_ context.Context, limit int) ([]*domain.OutboxEvent, error) {
	r.limit = limit
	return r.events, nil
}

func (r *outboxRepositoryStub) Update(_ context.Context, event *domain.OutboxEvent) error {
	copy := *event
	r.updated = append(r.updated, &copy)
	return nil
}

type publishedMessage struct {
	topic   string
	key     []byte
	payload []byte
}

type producerStub struct {
	err            error
	messages       []publishedMessage
	publishStarted chan struct{}
	release        chan struct{}
}

func (p *producerStub) Publish(_ context.Context, topic string, key []byte, payload []byte) error {
	p.messages = append(p.messages, publishedMessage{
		topic:   topic,
		key:     append([]byte(nil), key...),
		payload: append([]byte(nil), payload...),
	})
	if p.publishStarted != nil {
		close(p.publishStarted)
		<-p.release
	}
	return p.err
}

func (p *producerStub) Close() error {
	return nil
}

type transactorStub struct {
	calls int
}

func (t *transactorStub) WithinTransaction(ctx context.Context, fn func(context.Context) error) error {
	t.calls++
	return fn(ctx)
}
