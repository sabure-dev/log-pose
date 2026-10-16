package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"inventory_service/internal/domain"
)

func TestOrderCreatedEvent_UnmarshalJSON(t *testing.T) {
	rawJSON := `{
		"event_id": "8a0c2049-74d6-4e5a-8b1b-68936efc9df5",
		"event_type": "OrderCreated",
		"order_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
		"user_id": "123e4567-e89b-12d3-a456-426614174000",
		"total_amount": "3301.00",
		"shipping_address": "Main Street 100",
		"items": [
			{
				"product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"quantity": 2,
				"price": "1500.50"
			}
		],
		"occurred_at": "2026-10-15T22:30:00.000000+00:00"
	}`

	var event domain.OrderCreatedEvent
	if err := json.Unmarshal([]byte(rawJSON), &event); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	expectedEventID := uuid.MustParse("8a0c2049-74d6-4e5a-8b1b-68936efc9df5")
	if event.EventID != expectedEventID {
		t.Errorf("expected EventID %s, got %s", expectedEventID, event.EventID)
	}

	if event.TotalAmount != "3301.00" {
		t.Errorf("expected TotalAmount '3301.00', got %s", event.TotalAmount)
	}

	if event.ShippingAddress == nil || *event.ShippingAddress != "Main Street 100" {
		t.Errorf("expected ShippingAddress 'Main Street 100', got %v", event.ShippingAddress)
	}

	if len(event.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(event.Items))
	}

	if event.Items[0].Quantity != 2 {
		t.Errorf("expected quantity 2, got %d", event.Items[0].Quantity)
	}
}

func TestOrderCancelledEvent_UnmarshalJSON(t *testing.T) {
	rawJSON := `{
		"event_id": "9b1c3059-85e7-5f6b-9c2c-79047efd0ea6",
		"event_type": "OrderCancelled",
		"order_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
		"user_id": "123e4567-e89b-12d3-a456-426614174000",
		"items": [
			{
				"product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
				"quantity": 2
			}
		],
		"occurred_at": "2026-10-17T00:00:00.000000+00:00"
	}`

	var event domain.OrderCancelledEvent
	if err := json.Unmarshal([]byte(rawJSON), &event); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	expectedOrderID := uuid.MustParse("3fa85f64-5717-4562-b3fc-2c963f66afa6")
	if event.OrderID != expectedOrderID {
		t.Errorf("expected OrderID %s, got %s", expectedOrderID, event.OrderID)
	}

	if len(event.Items) != 1 || event.Items[0].Quantity != 2 {
		t.Errorf("expected 1 item with quantity 2, got %v", event.Items)
	}
}

func TestInventoryReservedEvent_MarshalJSON(t *testing.T) {
	eventID := uuid.New()
	orderID := uuid.New()

	event := domain.InventoryReservedEvent{
		EventID:   eventID,
		EventType: "InventoryReservationResult",
		OrderID:   orderID,
		Status:    "RESERVED",
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}

	var parsed map[string]interface{}
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("expected valid json, got %v", err)
	}

	if parsed["status"] != "RESERVED" {
		t.Errorf("expected status RESERVED, got %v", parsed["status"])
	}

	if _, ok := parsed["reason"]; ok {
		t.Errorf("expected reason to be omitted when empty, got %v", parsed["reason"])
	}
}
