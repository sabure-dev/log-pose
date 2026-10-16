package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
)

const maxDeliveryAttempts = 3

type OrderConsumer struct {
	ch       *amqp.Channel
	reserver repository.Reserver
}

func NewOrderConsumer(ch *amqp.Channel, reserver repository.Reserver) *OrderConsumer {
	return &OrderConsumer{ch: ch, reserver: reserver}
}

func (c *OrderConsumer) Run(ctx context.Context) error {
	if err := c.ch.ExchangeDeclare("logpose_dlx", "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare dlx exchange: %w", err)
	}

	dlq, err := c.ch.QueueDeclare("inventory_order_events_dlq", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare dlq: %w", err)
	}

	if err := c.ch.QueueBind(dlq.Name, "#", "logpose_dlx", false, nil); err != nil {
		return fmt.Errorf("failed to bind dlq: %w", err)
	}

	err = c.ch.ExchangeDeclare(
		"logpose_events",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	q, err := c.ch.QueueDeclare(
		"inventory_order_events_queue",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "logpose_dlx",
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	for _, routingKey := range []string{"order.created", "order.cancelled"} {
		err = c.ch.QueueBind(
			q.Name,
			routingKey,
			"logpose_events",
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("failed to bind queue to %s: %w", routingKey, err)
		}
	}

	if err := c.ch.Qos(20, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	msgs, err := c.ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to start consumer: %w", err)
	}

	notifyClose := c.ch.NotifyClose(make(chan *amqp.Error, 1))
	slog.Info("OrderConsumer started", "queue", q.Name)

	for {
		select {
		case <-ctx.Done():
			return nil
		case amqpErr, ok := <-notifyClose:
			if ok && amqpErr != nil {
				return fmt.Errorf("consumer amqp channel closed: %w", amqpErr)
			}
			return fmt.Errorf("consumer amqp channel closed")
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("consumer delivery channel closed")
			}
			c.handleDelivery(ctx, d)
		}
	}
}

func (c *OrderConsumer) handleDelivery(parentCtx context.Context, d amqp.Delivery) {
	processCtx, cancel := context.WithTimeout(context.WithoutCancel(parentCtx), 15*time.Second)
	defer cancel()

	switch d.RoutingKey {
	case "order.created":
		c.handleOrderCreated(processCtx, d)
	case "order.cancelled":
		c.handleOrderCancelled(processCtx, d)
	default:
		slog.Warn("Unhandled routing key", "routing_key", d.RoutingKey)
		_ = d.Nack(false, false)
	}
}

func (c *OrderConsumer) handleOrderCreated(ctx context.Context, d amqp.Delivery) {
	var event domain.OrderCreatedEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("Malformed OrderCreated payload", "error", err)
		_ = d.Nack(false, false)
		return
	}

	err := c.reserver.Reserve(ctx, event)
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyProcessed) {
			slog.Warn("Order already processed", "order_id", event.OrderID, "correlation_id", event.CorrelationID)
			_ = d.Ack(false)
			return
		}
		if errors.Is(err, repository.ErrOutOfStock) ||
			errors.Is(err, repository.ErrInvalidQuantity) ||
			errors.Is(err, repository.ErrPriceMismatch) {
			slog.Warn("Order rejected by inventory rules", "order_id", event.OrderID, "correlation_id", event.CorrelationID, "error", err)
			_ = d.Ack(false)
			return
		}
		c.retryOrDeadLetter(ctx, d, event.OrderID.String(), event.CorrelationID, err)
		return
	}

	slog.Info("Order items reserved", "order_id", event.OrderID, "correlation_id", event.CorrelationID)
	_ = d.Ack(false)
}

func (c *OrderConsumer) handleOrderCancelled(ctx context.Context, d amqp.Delivery) {
	var event domain.OrderCancelledEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("Malformed OrderCancelled payload", "error", err)
		_ = d.Nack(false, false)
		return
	}

	err := c.reserver.Release(ctx, event)
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyProcessed) {
			slog.Warn("Order cancellation already processed", "order_id", event.OrderID, "correlation_id", event.CorrelationID)
			_ = d.Ack(false)
			return
		}
		if errors.Is(err, repository.ErrCorruptedReservation) {
			slog.Error("Permanent reservation corruption detected, sending to DLQ", "order_id", event.OrderID, "correlation_id", event.CorrelationID, "error", err)
			_ = d.Nack(false, false)
			return
		}
		c.retryOrDeadLetter(ctx, d, event.OrderID.String(), event.CorrelationID, err)
		return
	}

	slog.Info("Order reservation released", "order_id", event.OrderID, "correlation_id", event.CorrelationID)
	_ = d.Ack(false)
}

func (c *OrderConsumer) retryOrDeadLetter(ctx context.Context, d amqp.Delivery, orderID, correlationID string, err error) {
	attempts := getRetryCount(d.Headers) + 1
	if attempts >= maxDeliveryAttempts {
		slog.Error("Max delivery attempts reached, sending to DLQ", "order_id", orderID, "correlation_id", correlationID, "attempts", attempts, "error", err)
		_ = d.Nack(false, false)
		return
	}

	slog.Warn("Transient error, scheduling retry", "order_id", orderID, "correlation_id", correlationID, "attempt", attempts, "error", err)
	if c.ch == nil {
		_ = d.Nack(false, true)
		return
	}

	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["x-retry-count"] = int32(attempts)

	pubErr := c.ch.PublishWithContext(
		ctx,
		d.Exchange,
		d.RoutingKey,
		false,
		false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  d.ContentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    d.MessageId,
			Body:         d.Body,
		},
	)
	if pubErr != nil {
		_ = d.Nack(false, true)
		return
	}
	_ = d.Ack(false)
}

func getRetryCount(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	switch v := headers["x-retry-count"].(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		return 0
	}
}
