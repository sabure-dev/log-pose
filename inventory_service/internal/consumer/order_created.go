package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
)

const (
	maxDeliveryAttempts = 3
	defaultRetryDelayMs = 2000
	retryConfirmTimeout = 5 * time.Second
	mainQueueName       = "inventory_order_events_queue"
	retryQueueName      = "inventory_order_events_retry_queue"
	dlqQueueName        = "inventory_order_events_dlq"
)

type DeferredConfirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

type AMQPChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	Confirm(noWait bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (*amqp.DeferredConfirmation, error)
}

type RetryPublishFunc func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (DeferredConfirmation, error)

type OrderConsumer struct {
	ch           AMQPChannel
	publishRetry RetryPublishFunc
	reserver     repository.Reserver
	retryDelayMs int
}

func NewOrderConsumer(ch AMQPChannel, reserver repository.Reserver) *OrderConsumer {
	c := &OrderConsumer{
		ch:           ch,
		reserver:     reserver,
		retryDelayMs: defaultRetryDelayMs,
	}
	if ch != nil {
		c.publishRetry = func(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (DeferredConfirmation, error) {
			return ch.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
		}
	}
	return c
}

func (c *OrderConsumer) SetRetryDelayMs(delayMs int) {
	if delayMs > 0 {
		c.retryDelayMs = delayMs
	}
}

func (c *OrderConsumer) SetRetryPublisher(fn RetryPublishFunc) {
	c.publishRetry = fn
}

func (c *OrderConsumer) Run(ctx context.Context) error {
	if err := c.ch.ExchangeDeclare("logpose_dlx", "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare dlx exchange: %w", err)
	}

	dlq, err := c.ch.QueueDeclare(dlqQueueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare dlq: %w", err)
	}

	// Bind DLQ to:
	// - dlqQueueName ("inventory_order_events_dlq")
	// - mainQueueName ("inventory_order_events_queue"): messages returning from retryQueueName have routing key = mainQueueName
	// - "order.#": messages rejected on their initial delivery before entering retryQueueName
	for _, dlqKey := range []string{dlqQueueName, mainQueueName, "order.#"} {
		if err := c.ch.QueueBind(dlq.Name, dlqKey, "logpose_dlx", false, nil); err != nil {
			return fmt.Errorf("failed to bind dlq to %s: %w", dlqKey, err)
		}
	}

	// Declare delay/retry queue that routes expired messages back to mainQueueName via default exchange.
	_, err = c.ch.QueueDeclare(
		retryQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": mainQueueName,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare retry queue: %w", err)
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
		mainQueueName,
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

	for _, routingKey := range []string{"order.created", "order.cancelled", "order.completed"} {
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

	if err := c.ch.Confirm(false); err != nil {
		return fmt.Errorf("failed to enable publisher confirms on consumer channel: %w", err)
	}

	if err := c.ch.Qos(20, 0, false); err != nil {
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	notifyClose := c.ch.NotifyClose(make(chan *amqp.Error, 1))

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

	routingKey := effectiveRoutingKey(d)

	switch routingKey {
	case "order.created":
		c.handleOrderCreated(processCtx, d)
	case "order.cancelled":
		c.handleOrderCancelled(processCtx, d)
	case "order.completed":
		c.handleOrderCompleted(processCtx, d)
	default:
		slog.Warn("Unhandled routing key", "routing_key", routingKey)
		_ = d.Nack(false, false)
	}
}

func effectiveRoutingKey(d amqp.Delivery) string {
	if d.Headers != nil {
		if orig, ok := d.Headers["x-original-routing-key"].(string); ok && orig != "" {
			return orig
		}
	}
	return d.RoutingKey
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
			errors.Is(err, repository.ErrProductNotFound) ||
			errors.Is(err, repository.ErrInvalidQuantity) ||
			errors.Is(err, repository.ErrPriceMismatch) ||
			errors.Is(err, repository.ErrInvalidEvent) {
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
		if errors.Is(err, repository.ErrInvalidEvent) || errors.Is(err, repository.ErrCorruptedReservation) {
			slog.Error("Permanent reservation error on cancel, sending to DLQ", "order_id", event.OrderID, "correlation_id", event.CorrelationID, "error", err)
			_ = d.Nack(false, false)
			return
		}
		c.retryOrDeadLetter(ctx, d, event.OrderID.String(), event.CorrelationID, err)
		return
	}

	slog.Info("Order reservation released", "order_id", event.OrderID, "correlation_id", event.CorrelationID)
	_ = d.Ack(false)
}

func (c *OrderConsumer) handleOrderCompleted(ctx context.Context, d amqp.Delivery) {
	var event domain.OrderCompletedEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("Malformed OrderCompleted payload", "error", err)
		_ = d.Nack(false, false)
		return
	}

	err := c.reserver.Confirm(ctx, event)
	if err != nil {
		if errors.Is(err, repository.ErrInvalidEvent) || errors.Is(err, repository.ErrCorruptedReservation) {
			slog.Error("Permanent reservation error on complete, sending to DLQ", "order_id", event.OrderID, "correlation_id", event.CorrelationID, "error", err)
			_ = d.Nack(false, false)
			return
		}
		c.retryOrDeadLetter(ctx, d, event.OrderID.String(), event.CorrelationID, err)
		return
	}

	slog.Info("Order reservation confirmed and deducted", "order_id", event.OrderID, "correlation_id", event.CorrelationID)
	_ = d.Ack(false)
}

func (c *OrderConsumer) retryOrDeadLetter(ctx context.Context, d amqp.Delivery, orderID, correlationID string, err error) {
	attempts := getRetryCount(d.Headers) + 1
	if attempts >= maxDeliveryAttempts {
		slog.Error("Max delivery attempts reached, sending to DLQ", "order_id", orderID, "correlation_id", correlationID, "attempts", attempts, "error", err)
		_ = d.Nack(false, false)
		return
	}

	slog.Warn("Transient error, scheduling delayed retry", "order_id", orderID, "correlation_id", correlationID, "attempt", attempts, "delay_ms", c.retryDelayMs, "error", err)

	headers := amqp.Table{}
	for k, v := range d.Headers {
		headers[k] = v
	}
	headers["x-retry-count"] = int32(attempts)
	headers["x-original-routing-key"] = effectiveRoutingKey(d)

	// Publish to retryQueueName via default exchange ("") with uniform TTL to avoid FIFO head-of-line blocking,
	// and wait for broker publisher confirm before acknowledging the original delivery.
	dc, pubErr := c.publishRetry(
		ctx,
		"",
		retryQueueName,
		false,
		false,
		amqp.Publishing{
			Headers:      headers,
			ContentType:  d.ContentType,
			DeliveryMode: amqp.Persistent,
			MessageId:    d.MessageId,
			Expiration:   strconv.Itoa(c.retryDelayMs),
			Body:         d.Body,
		},
	)
	if pubErr != nil {
		_ = d.Nack(false, true)
		return
	}

	cctx, cancel := context.WithTimeout(ctx, retryConfirmTimeout)
	ok, confirmErr := dc.WaitContext(cctx)
	cancel()
	if confirmErr != nil || !ok {
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
