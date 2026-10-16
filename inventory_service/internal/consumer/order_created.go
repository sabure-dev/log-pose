package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
)

type OrderConsumer struct {
	ch       *amqp.Channel
	reserver repository.Reserver
	wg       sync.WaitGroup
}

func NewOrderConsumer(ch *amqp.Channel, reserver repository.Reserver) *OrderConsumer {
	return &OrderConsumer{ch: ch, reserver: reserver}
}

func (c *OrderConsumer) Run(ctx context.Context) error {
	err := c.ch.ExchangeDeclare(
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
		nil,
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
			c.wg.Wait()
			return ctx.Err()
		case amqpErr, ok := <-notifyClose:
			if ok && amqpErr != nil {
				return fmt.Errorf("amqp channel closed: %w", amqpErr)
			}
			return fmt.Errorf("amqp channel closed")
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("amqp delivery channel closed")
			}
			c.wg.Add(1)
			go func(delivery amqp.Delivery) {
				defer c.wg.Done()
				c.handleDelivery(ctx, delivery)
			}(d)
		}
	}
}

func (c *OrderConsumer) Wait() {
	c.wg.Wait()
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
		_ = d.Ack(false)
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
			slog.Warn("Order already processed", "order_id", event.OrderID)
			_ = d.Ack(false)
			return
		}
		if errors.Is(err, repository.ErrOutOfStock) || errors.Is(err, repository.ErrInvalidQuantity) {
			slog.Warn("Order rejected by inventory rules", "order_id", event.OrderID, "error", err)
			_ = d.Ack(false)
			return
		}
		slog.Error("Transient error during reservation", "order_id", event.OrderID, "error", err)
		_ = d.Nack(false, true)
		return
	}

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
			slog.Warn("Order cancellation already processed", "order_id", event.OrderID)
			_ = d.Ack(false)
			return
		}
		slog.Error("Transient error during release", "order_id", event.OrderID, "error", err)
		_ = d.Nack(false, true)
		return
	}

	_ = d.Ack(false)
}
