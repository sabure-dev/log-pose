package domain

import "github.com/google/uuid"

type OrderItemPayload struct {
	ProductID uuid.UUID `json:"product_id"`
	Quantity  int       `json:"quantity"`
	Price     string    `json:"price,omitempty"`
}

type OrderCreatedEvent struct {
	EventID         uuid.UUID          `json:"event_id"`
	CorrelationID   string             `json:"correlation_id,omitempty"`
	EventType       string             `json:"event_type"`
	OrderID         uuid.UUID          `json:"order_id"`
	UserID          uuid.UUID          `json:"user_id"`
	TotalAmount     string             `json:"total_amount"`
	ShippingAddress *string            `json:"shipping_address,omitempty"`
	Items           []OrderItemPayload `json:"items"`
	OccurredAt      string             `json:"occurred_at"`
}

type OrderCancelledEvent struct {
	EventID       uuid.UUID          `json:"event_id"`
	CorrelationID string             `json:"correlation_id,omitempty"`
	EventType     string             `json:"event_type"`
	OrderID       uuid.UUID          `json:"order_id"`
	UserID        uuid.UUID          `json:"user_id"`
	Items         []OrderItemPayload `json:"items"`
	OccurredAt    string             `json:"occurred_at"`
}

type OrderCompletedEvent struct {
	EventID       uuid.UUID          `json:"event_id"`
	CorrelationID string             `json:"correlation_id,omitempty"`
	EventType     string             `json:"event_type"`
	OrderID       uuid.UUID          `json:"order_id"`
	UserID        uuid.UUID          `json:"user_id"`
	Items         []OrderItemPayload `json:"items"`
	OccurredAt    string             `json:"occurred_at"`
}

type InventoryReservationResultEvent struct {
	EventID       uuid.UUID `json:"event_id"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	EventType     string    `json:"event_type"`
	OrderID       uuid.UUID `json:"order_id"`
	Status        string    `json:"status"`
	Reason        string    `json:"reason,omitempty"`
}

type InventoryReservedEvent = InventoryReservationResultEvent
