package domain_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"inventory_service/internal/domain"
)

func readContract(filename string) ([]byte, error) {
	candidates := []string{
		filepath.Join("..", "..", "..", "docs", "contracts", filename),
		filepath.Join("..", "..", "docs", "contracts", filename),
		filepath.Join("docs", "contracts", filename),
	}
	for _, p := range candidates {
		if data, err := os.ReadFile(p); err == nil {
			return data, nil
		}
	}
	return nil, os.ErrNotExist
}

func assertJSONKeysMatch(t *testing.T, contractBytes []byte, producedBytes []byte) {
	t.Helper()
	var contractMap map[string]any
	var producedMap map[string]any
	if err := json.Unmarshal(contractBytes, &contractMap); err != nil {
		t.Fatalf("failed to unmarshal contract JSON map: %v", err)
	}
	if err := json.Unmarshal(producedBytes, &producedMap); err != nil {
		t.Fatalf("failed to unmarshal produced JSON map: %v", err)
	}
	for k := range contractMap {
		if _, ok := producedMap[k]; !ok {
			t.Errorf("contract key %q missing from produced payload", k)
		}
	}
	for k := range producedMap {
		if _, ok := contractMap[k]; !ok {
			t.Errorf("produced key %q not documented in contract", k)
		}
	}
}

func TestContracts_OrderCreated(t *testing.T) {
	data, err := readContract("order_created.json")
	if err != nil {
		t.Fatalf("failed to read contract file: %v", err)
	}

	var event domain.OrderCreatedEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal order_created.json: %v", err)
	}

	expectedEventID := uuid.MustParse("8a0c2049-74d6-4e5a-8b1b-68936efc9df5")
	if event.EventID != expectedEventID {
		t.Errorf("expected EventID %s, got %s", expectedEventID, event.EventID)
	}

	if event.TotalAmount != "3001.00" {
		t.Errorf("expected TotalAmount 3001.00, got %s", event.TotalAmount)
	}

	if event.ShippingAddress == nil || *event.ShippingAddress != "Main Street 100, Apt 4B" {
		t.Errorf("expected shipping address 'Main Street 100, Apt 4B', got %v", event.ShippingAddress)
	}

	if len(event.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(event.Items))
	}

	reMarshaled, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal OrderCreatedEvent: %v", err)
	}
	assertJSONKeysMatch(t, data, reMarshaled)
}

func TestContracts_OrderCancelled(t *testing.T) {
	data, err := readContract("order_cancelled.json")
	if err != nil {
		t.Fatalf("failed to read contract file: %v", err)
	}

	var event domain.OrderCancelledEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal order_cancelled.json: %v", err)
	}

	expectedOrderID := uuid.MustParse("3fa85f64-5717-4562-b3fc-2c963f66afa6")
	if event.OrderID != expectedOrderID {
		t.Errorf("expected OrderID %s, got %s", expectedOrderID, event.OrderID)
	}

	reMarshaled, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal OrderCancelledEvent: %v", err)
	}
	assertJSONKeysMatch(t, data, reMarshaled)
}

func TestContracts_OrderCompleted(t *testing.T) {
	data, err := readContract("order_completed.json")
	if err != nil {
		t.Fatalf("failed to read contract file: %v", err)
	}

	var event domain.OrderCompletedEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal order_completed.json: %v", err)
	}

	if event.EventType != "OrderCompleted" {
		t.Errorf("expected EventType OrderCompleted, got %s", event.EventType)
	}

	reMarshaled, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("failed to marshal OrderCompletedEvent: %v", err)
	}
	assertJSONKeysMatch(t, data, reMarshaled)
}

func TestContracts_InventoryReserved(t *testing.T) {
	data, err := readContract("inventory_reserved.json")
	if err != nil {
		t.Fatalf("failed to read contract file: %v", err)
	}

	var event domain.InventoryReservationResultEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal inventory_reserved.json: %v", err)
	}

	if event.Status != "RESERVED" {
		t.Errorf("expected status RESERVED, got %s", event.Status)
	}

	produced := domain.InventoryReservationResultEvent{
		EventID:       event.EventID,
		CorrelationID: event.CorrelationID,
		EventType:     "InventoryReservationResult",
		OrderID:       event.OrderID,
		Status:        "RESERVED",
	}
	producedBytes, _ := json.Marshal(produced)
	assertJSONKeysMatch(t, data, producedBytes)
}

func TestContracts_InventoryFailed(t *testing.T) {
	data, err := readContract("inventory_failed.json")
	if err != nil {
		t.Fatalf("failed to read contract file: %v", err)
	}

	var event domain.InventoryReservationResultEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal inventory_failed.json: %v", err)
	}

	if event.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", event.Status)
	}

	if event.Reason != "insufficient stock" {
		t.Errorf("expected reason 'insufficient stock', got %s", event.Reason)
	}

	produced := domain.InventoryReservationResultEvent{
		EventID:       event.EventID,
		CorrelationID: event.CorrelationID,
		EventType:     "InventoryReservationResult",
		OrderID:       event.OrderID,
		Status:        "FAILED",
		Reason:        "insufficient stock",
	}
	producedBytes, _ := json.Marshal(produced)
	assertJSONKeysMatch(t, data, producedBytes)
}
