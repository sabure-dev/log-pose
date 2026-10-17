package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
)

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

func (m *mockAMQPChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return nil, nil
}

func (m *mockAMQPChannel) NotifyClose(c chan *amqp.Error) chan *amqp.Error {
	return c
}

func (m *mockAMQPChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if m.publishErr != nil {
		return m.publishErr
	}
	m.published = append(m.published, publishedCall{
		exchange:   exchange,
		routingKey: key,
		msg:        msg,
	})
	return nil
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
	mockCh := &mockAMQPChannel{}
	mockRes := &mockReserver{}
	c := NewOrderConsumer(mockCh, mockRes)

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
	c := NewOrderConsumer(&mockAMQPChannel{}, &mockReserver{})

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
		c := NewOrderConsumer(&mockAMQPChannel{}, mockRes)

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

	// Attempt 1: publishes to retryQueueName via default exchange "" with Expiration="1000" and x-retry-count=1
	mockAck1 := &mockAcknowledger{}
	mockCh1 := &mockAMQPChannel{}
	c1 := NewOrderConsumer(mockCh1, mockRes)

	d1 := amqp.Delivery{
		Acknowledger: mockAck1,
		Body:         body,
		RoutingKey:   "order.created",
	}
	c1.handleDelivery(context.Background(), d1)

	if !mockAck1.acked {
		t.Fatalf("expected original message to be acked after publishing to retry queue")
	}
	if len(mockCh1.published) != 1 {
		t.Fatalf("expected 1 retry publish, got %d", len(mockCh1.published))
	}
	pub1 := mockCh1.published[0]
	if pub1.exchange != "" || pub1.routingKey != retryQueueName {
		t.Errorf("expected retry published to default exchange '' and key %q, got exchange=%q key=%q", retryQueueName, pub1.exchange, pub1.routingKey)
	}
	if pub1.msg.Expiration != "1000" {
		t.Errorf("expected 1000ms expiration on attempt 1, got %q", pub1.msg.Expiration)
	}
	if pub1.msg.Headers["x-retry-count"] != int32(1) {
		t.Errorf("expected x-retry-count=1, got %v", pub1.msg.Headers["x-retry-count"])
	}
	if pub1.msg.Headers["x-original-routing-key"] != "order.created" {
		t.Errorf("expected x-original-routing-key='order.created', got %v", pub1.msg.Headers["x-original-routing-key"])
	}

	// Attempt 3 (x-retry-count=2 already): reaches maxDeliveryAttempts (3), sends to DLQ via Nack(requeue=false)
	mockAck3 := &mockAcknowledger{}
	mockCh3 := &mockAMQPChannel{}
	c3 := NewOrderConsumer(mockCh3, mockRes)

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
	c := NewOrderConsumer(&mockAMQPChannel{}, mockRes)

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
	c := NewOrderConsumer(&mockAMQPChannel{}, mockRes)

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
