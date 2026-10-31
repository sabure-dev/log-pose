package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
)

type stubDeferredConfirm struct {
	ok  bool
	err error
}

func (s stubDeferredConfirm) WaitContext(ctx context.Context) (bool, error) {
	return s.ok, s.err
}

type mockAcknowledger struct {
	acked   bool
	nacked  bool
	requeue bool
}

func (m *mockAcknowledger) Ack(tag uint64, multiple bool) error {
	m.acked = true
	return nil
}

func (m *mockAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	m.nacked = true
	m.requeue = requeue
	return nil
}

func (m *mockAcknowledger) Reject(tag uint64, requeue bool) error {
	m.nacked = true
	m.requeue = requeue
	return nil
}

type publishedCall struct {
	exchange   string
	routingKey string
	msg        amqp.Publishing
}

type mockAMQPChannel struct {
	published  []publishedCall
	publishErr error
	confirmOK  bool
}

func newMockAMQPChannel() *mockAMQPChannel {
	return &mockAMQPChannel{confirmOK: true}
}

func (m *mockAMQPChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	return nil
}

func (m *mockAMQPChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	return amqp.Queue{Name: name}, nil
}

func (m *mockAMQPChannel) QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error {
	return nil
}

func (m *mockAMQPChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return nil
}

func (m *mockAMQPChannel) Confirm(noWait bool) error {
	return nil
}

func (m *mockAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, nil
}

func (m *mockAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return c
}

func (m *mockAMQPChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error) {
	if m.publishErr != nil {
		return nil, m.publishErr
	}
	m.published = append(m.published, publishedCall{
		exchange:   exchange,
		routingKey: key,
		msg:        msg,
	})
	return nil, nil
}

func newConsumerWithMockChannel(mockCh *mockAMQPChannel, res repository.Reserver) *OrderConsumer {
	c := NewOrderConsumer(mockCh, res)
	c.SetRetryPublisher(func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (DeferredConfirmation, error) {
		if mockCh.publishErr != nil {
			return nil, mockCh.publishErr
		}
		mockCh.published = append(mockCh.published, publishedCall{
			exchange:   exchange,
			routingKey: key,
			msg:        msg,
		})
		return stubDeferredConfirm{ok: mockCh.confirmOK, err: nil}, nil
	})
	return c
}

type mockReserver struct {
	reserveFunc func(ctx context.Context, event domain.OrderCreatedEvent) error
	releaseFunc func(ctx context.Context, event domain.OrderCancelledEvent) error
	confirmFunc func(ctx context.Context, event domain.OrderCompletedEvent) error
}

func (m *mockReserver) Reserve(ctx context.Context, event domain.OrderCreatedEvent) error {
	if m.reserveFunc != nil {
		return m.reserveFunc(ctx, event)
	}
	return nil
}

func (m *mockReserver) Release(ctx context.Context, event domain.OrderCancelledEvent) error {
	if m.releaseFunc != nil {
		return m.releaseFunc(ctx, event)
	}
	return nil
}

func (m *mockReserver) Confirm(ctx context.Context, event domain.OrderCompletedEvent) error {
	if m.confirmFunc != nil {
		return m.confirmFunc(ctx, event)
	}
	return nil
}

func TestOrderConsumer_HandleOrderCreated_Success(t *testing.T) {
	mockAck := &mockAcknowledger{}
	mockCh := newMockAMQPChannel()
	mockRes := &mockReserver{}
	c := newConsumerWithMockChannel(mockCh, mockRes)

	event := domain.OrderCreatedEvent{
		EventID: uuid.New(),
		OrderID: uuid.New(),
	}
	body, _ := json.Marshal(event)

	d := amqp.Delivery{
		Acknowledger: mockAck,
		Body:         body,
		RoutingKey:   "order.created",
	}

	c.handleDelivery(context.Background(), d)

	if !mockAck.acked {
		t.Errorf("expected message to be acked")
	}
}

func TestOrderConsumer_HandleOrderCreated_MalformedJSON(t *testing.T) {
	mockAck := &mockAcknowledger{}
	c := newConsumerWithMockChannel(newMockAMQPChannel(), &mockReserver{})

	d := amqp.Delivery{
		Acknowledger: mockAck,
		Body:         []byte("invalid json"),
		RoutingKey:   "order.created",
	}

	c.handleDelivery(context.Background(), d)

	if !mockAck.nacked || mockAck.requeue {
		t.Errorf("expected malformed message to be nacked without requeue")
	}
}

func TestOrderConsumer_HandleOrderCreated_DomainRejections(t *testing.T) {
	domainErrs := []error{
		repository.ErrOutOfStock,
		repository.ErrProductNotFound,
		repository.ErrPriceMismatch,
		repository.ErrInvalidQuantity,
		repository.ErrInvalidEvent,
		repository.ErrAlreadyProcessed,
	}

	for _, domainErr := range domainErrs {
		mockAck := &mockAcknowledger{}
		mockRes := &mockReserver{
			reserveFunc: func(ctx context.Context, event domain.OrderCreatedEvent) error {
				return domainErr
			},
		}
		c := newConsumerWithMockChannel(newMockAMQPChannel(), mockRes)

		event := domain.OrderCreatedEvent{
			EventID: uuid.New(),
			OrderID: uuid.New(),
		}
		body, _ := json.Marshal(event)

		d := amqp.Delivery{
			Acknowledger: mockAck,
			Body:         body,
			RoutingKey:   "order.created",
		}

		c.handleDelivery(context.Background(), d)

		if !mockAck.acked {
			t.Errorf("expected domain error %v to be acked", domainErr)
		}
	}
}

func TestOrderConsumer_RetryOrDeadLetter_DelayedRetryAndDLQ(t *testing.T) {
	mockRes := &mockReserver{
		reserveFunc: func(ctx context.Context, event domain.OrderCreatedEvent) error {
			return errors.New("db connection reset")
		},
	}

	event := domain.OrderCreatedEvent{
		EventID: uuid.New(),
		OrderID: uuid.New(),
	}
	body, _ := json.Marshal(event)

	mockAck1 := &mockAcknowledger{}
	mockCh1 := newMockAMQPChannel()
	c1 := newConsumerWithMockChannel(mockCh1, mockRes)

	d1 := amqp.Delivery{
		Acknowledger: mockAck1,
		Body:         body,
		RoutingKey:   "order.created",
	}
	c1.handleDelivery(context.Background(), d1)

	if !mockAck1.acked {
		t.Fatalf("expected original message to be acked after confirmed publish to retry queue")
	}
	if len(mockCh1.published) != 1 {
		t.Fatalf("expected 1 retry publish, got %d", len(mockCh1.published))
	}
	pub1 := mockCh1.published[0]
	if pub1.exchange != "" || pub1.routingKey != retryQueueName {
		t.Errorf("expected retry published to default exchange '' and key %q, got exchange=%q key=%q", retryQueueName, pub1.exchange, pub1.routingKey)
	}
	if pub1.msg.Expiration != "2000" {
		t.Errorf("expected 2000ms expiration, got %q", pub1.msg.Expiration)
	}
	if pub1.msg.Headers["x-retry-count"] != int32(1) {
		t.Errorf("expected x-retry-count=1, got %v", pub1.msg.Headers["x-retry-count"])
	}
	if pub1.msg.Headers["x-original-routing-key"] != "order.created" {
		t.Errorf("expected x-original-routing-key='order.created', got %v", pub1.msg.Headers["x-original-routing-key"])
	}

	mockAck3 := &mockAcknowledger{}
	mockCh3 := newMockAMQPChannel()
	c3 := newConsumerWithMockChannel(mockCh3, mockRes)

	d3 := amqp.Delivery{
		Acknowledger: mockAck3,
		Body:         body,
		RoutingKey:   mainQueueName,
		Headers: amqp.Table{
			"x-retry-count":          int32(2),
			"x-original-routing-key": "order.created",
		},
	}
	c3.handleDelivery(context.Background(), d3)

	if !mockAck3.nacked || mockAck3.requeue {
		t.Fatalf("expected 3rd failed attempt to be Nacked with requeue=false (DLQ)")
	}
	if len(mockCh3.published) != 0 {
		t.Fatalf("expected no retry publish when max attempts reached")
	}
}

func TestOrderConsumer_HandleOrderCancelled_Success(t *testing.T) {
	mockAck := &mockAcknowledger{}
	mockRes := &mockReserver{}
	c := newConsumerWithMockChannel(newMockAMQPChannel(), mockRes)

	event := domain.OrderCancelledEvent{
		EventID: uuid.New(),
		OrderID: uuid.New(),
	}
	body, _ := json.Marshal(event)

	d := amqp.Delivery{
		Acknowledger: mockAck,
		Body:         body,
		RoutingKey:   "order.cancelled",
	}

	c.handleDelivery(context.Background(), d)

	if !mockAck.acked {
		t.Errorf("expected cancellation to be acked")
	}
}

func TestOrderConsumer_HandleOrderCompleted_Success(t *testing.T) {
	mockAck := &mockAcknowledger{}
	confirmed := false
	mockRes := &mockReserver{
		confirmFunc: func(ctx context.Context, event domain.OrderCompletedEvent) error {
			confirmed = true
			return nil
		},
	}
	c := newConsumerWithMockChannel(newMockAMQPChannel(), mockRes)

	event := domain.OrderCompletedEvent{
		EventID: uuid.New(),
		OrderID: uuid.New(),
	}
	body, _ := json.Marshal(event)

	d := amqp.Delivery{
		Acknowledger: mockAck,
		Body:         body,
		RoutingKey:   "order.completed",
	}

	c.handleDelivery(context.Background(), d)

	if !confirmed || !mockAck.acked {
		t.Errorf("expected order.completed to call Confirm and Ack")
	}
}

func TestOrderConsumer_AMQPTopology_RetryAndDLQ_Integration(t *testing.T) {
	rmqURL := os.Getenv("TEST_RABBITMQ_URL")
	if rmqURL == "" {
		t.Skip("TEST_RABBITMQ_URL not set, skipping live RabbitMQ topology integration test")
	}

	conn, err := amqp.Dial(rmqURL)
	if err != nil {
		t.Fatalf("failed to connect to rabbitmq: %v", err)
	}
	defer conn.Close()

	setupCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open setup channel: %v", err)
	}
	defer setupCh.Close()

	_, _ = setupCh.QueueDelete(mainQueueName, false, false, false)
	_, _ = setupCh.QueueDelete(retryQueueName, false, false, false)
	_, _ = setupCh.QueueDelete(dlqQueueName, false, false, false)

	var attempts atomic.Int32
	mockRes := &mockReserver{
		reserveFunc: func(ctx context.Context, event domain.OrderCreatedEvent) error {
			attempts.Add(1)
			return errors.New("simulated transient database outage")
		},
	}

	consumerCh, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open consumer channel: %v", err)
	}
	defer consumerCh.Close()

	c := NewOrderConsumer(consumerCh, mockRes)
	c.SetRetryDelayMs(100) // 100ms retry TTL for integration test

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Run(ctx)
	}()

	time.Sleep(150 * time.Millisecond)

	orderID := uuid.New()
	ev := domain.OrderCreatedEvent{
		EventID:   uuid.New(),
		EventType: "OrderCreated",
		OrderID:   orderID,
		UserID:    uuid.New(),
	}
	body, _ := json.Marshal(ev)

	err = setupCh.PublishWithContext(
		ctx,
		"logpose_events",
		"order.created",
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    ev.EventID.String(),
			Body:         body,
		},
	)
	if err != nil {
		t.Fatalf("failed to publish test event: %v", err)
	}

	dlqMsgs, err := setupCh.Consume(dlqQueueName, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("failed to consume from DLQ: %v", err)
	}

	select {
	case msg := <-dlqMsgs:
		var dlqEv domain.OrderCreatedEvent
		if err := json.Unmarshal(msg.Body, &dlqEv); err != nil {
			t.Fatalf("failed to unmarshal DLQ message: %v", err)
		}
		if dlqEv.OrderID != orderID {
			t.Fatalf("expected DLQ message OrderID %s, got %s", orderID, dlqEv.OrderID)
		}
		if attempts.Load() != 3 {
			t.Fatalf("expected exactly 3 delivery attempts before DLQ, got %d", attempts.Load())
		}
	case err := <-errCh:
		t.Fatalf("consumer background worker failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for exhausted message to arrive in %s (attempts=%d)", dlqQueueName, attempts.Load())
	}
}
