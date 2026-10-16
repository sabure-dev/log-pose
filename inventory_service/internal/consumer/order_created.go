package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"inventory_service/internal/domain"
	"inventory_service/internal/repository"
)

type OrderConsumer struct {
	ch   *amqp.Channel
	repo *repository.InventoryRepository
	wg   sync.WaitGroup
}

func NewOrderConsumer(ch *amqp.Channel, repo *repository.InventoryRepository) *OrderConsumer {
	return &OrderConsumer{ch: ch, repo: repo}
}

func (c *OrderConsumer) Start(ctx context.Context) error {
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
		return err
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
		return err
	}

	err = c.ch.QueueBind(
		q.Name,
		"order.created",
		"logpose_events",
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = c.ch.QueueBind(
		q.Name,
		"order.cancelled",
		"logpose_events",
		false,
		nil,
	)
	if err != nil {
		return err
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
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}
				c.handleMessage(ctx, d)
			}
		}
	}()

	slog.Info("Inventory OrderConsumer started", "queue", q.Name)
	return nil
}

func (c *OrderConsumer) Wait() {
	c.wg.Wait()
}

func (c *OrderConsumer) handleMessage(ctx context.Context, d amqp.Delivery) {
	switch d.RoutingKey {
	case "order.created":
		c.handleOrderCreated(ctx, d)
	case "order.cancelled":
		c.handleOrderCancelled(ctx, d)
	default:
		slog.Warn("Unhandled routing key received", "routing_key", d.RoutingKey)
		_ = d.Ack(false)
	}
}

func (c *OrderConsumer) handleOrderCreated(ctx context.Context, d amqp.Delivery) {
	var event domain.OrderCreatedEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("Failed to unmarshal OrderCreated payload", "error", err)
		_ = d.Nack(false, false)
		return
	}

	slog.Info("Processing OrderCreated event", "order_id", event.OrderID, "event_id", event.EventID)

	err := c.repo.ReserveItems(ctx, event.EventID, event.Items)
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyProcessed) {
			slog.Warn("Event already processed, acknowledging duplicate", "event_id", event.EventID)
			_ = d.Ack(false)
			return
		}

		if errors.Is(err, repository.ErrOutOfStock) {
			slog.Warn("Order reservation failed due to insufficient stock", "order_id", event.OrderID)
			c.publishResult(ctx, event.OrderID, "FAILED", err.Error())
			_ = d.Ack(false)
			return
		}

		slog.Error("Transient error reserving items", "order_id", event.OrderID, "error", err)
		_ = d.Nack(false, true)
		return
	}

	slog.Info("Items reserved successfully", "order_id", event.OrderID)
	c.publishResult(ctx, event.OrderID, "RESERVED", "")
	_ = d.Ack(false)
}

func (c *OrderConsumer) handleOrderCancelled(ctx context.Context, d amqp.Delivery) {
	var event domain.OrderCancelledEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		slog.Error("Failed to unmarshal OrderCancelled payload", "error", err)
		_ = d.Nack(false, false)
		return
	}

	slog.Info("Processing OrderCancelled event, releasing stock", "order_id", event.OrderID, "event_id", event.EventID)

	err := c.repo.ReleaseItems(ctx, event.EventID, event.Items)
	if err != nil {
		if errors.Is(err, repository.ErrAlreadyProcessed) {
			slog.Warn("Event already processed, acknowledging duplicate", "event_id", event.EventID)
			_ = d.Ack(false)
			return
		}

		slog.Error("Transient error releasing items", "order_id", event.OrderID, "error", err)
		_ = d.Nack(false, true)
		return
	}

	slog.Info("Items released successfully", "order_id", event.OrderID)
	_ = d.Ack(false)
}

func (c *OrderConsumer) publishResult(ctx context.Context, orderID uuid.UUID, status, reason string) {
	res := domain.InventoryReservedEvent{
		EventID:   uuid.New(),
		EventType: "InventoryReservationResult",
		OrderID:   orderID,
		Status:    status,
		Reason:    reason,
	}

	body, err := json.Marshal(res)
	if err != nil {
		slog.Error("Failed to marshal reservation result", "error", err)
		return
	}

	routingKey := "inventory.reserved"
	if status == "FAILED" {
		routingKey = "inventory.failed"
	}

	err = c.ch.PublishWithContext(
		ctx,
		"logpose_events",
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		slog.Error("Failed to publish inventory reservation result", "routing_key", routingKey, "error", err)
	}
}
