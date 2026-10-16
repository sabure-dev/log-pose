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

type mockReserver struct {
	reserveFunc func(ctx context.Context, event domain.OrderCreatedEvent) error
	releaseFunc func(ctx context.Context, event domain.OrderCancelledEvent) error
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

func TestOrderConsumer_HandleOrderCreated_Success(t *testing.T) {
	mockAck := &mockAcknowledger{}
	mockRes := &mockReserver{}
	c := NewOrderConsumer(nil, mockRes)

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
	c := NewOrderConsumer(nil, &mockReserver{})

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

func TestOrderConsumer_HandleOrderCreated_OutOfStock(t *testing.T) {
	mockAck := &mockAcknowledger{}
	mockRes := &mockReserver{
		reserveFunc: func(ctx context.Context, event domain.OrderCreatedEvent) error {
			return repository.ErrOutOfStock
		},
	}
	c := NewOrderConsumer(nil, mockRes)

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
		t.Errorf("expected out of stock to be acked because outbox event was recorded")
	}
}

func TestOrderConsumer_HandleOrderCreated_TransientError(t *testing.T) {
	mockAck := &mockAcknowledger{}
	mockRes := &mockReserver{
		reserveFunc: func(ctx context.Context, event domain.OrderCreatedEvent) error {
			return errors.New("db network error")
		},
	}
	c := NewOrderConsumer(nil, mockRes)

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

	if !mockAck.nacked || !mockAck.requeue {
		t.Errorf("expected transient error to be nacked with requeue=true")
	}
}

func TestOrderConsumer_HandleOrderCancelled_Success(t *testing.T) {
	mockAck := &mockAcknowledger{}
	mockRes := &mockReserver{}
	c := NewOrderConsumer(nil, mockRes)

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
