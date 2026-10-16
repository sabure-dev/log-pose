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

	if event.TotalAmount != "3301.00" {
		t.Errorf("expected TotalAmount 3301.00, got %s", event.TotalAmount)
	}

	if event.ShippingAddress == nil || *event.ShippingAddress != "Main Street 100, Apt 4B" {
		t.Errorf("expected shipping address 'Main Street 100, Apt 4B', got %v", event.ShippingAddress)
	}

	if len(event.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(event.Items))
	}
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
}

func TestContracts_InventoryReserved(t *testing.T) {
	data, err := readContract("inventory_reserved.json")
	if err != nil {
		t.Fatalf("failed to read contract file: %v", err)
	}

	var event domain.InventoryReservedEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal inventory_reserved.json: %v", err)
	}

	if event.Status != "RESERVED" {
		t.Errorf("expected status RESERVED, got %s", event.Status)
	}
}

func TestContracts_InventoryFailed(t *testing.T) {
	data, err := readContract("inventory_failed.json")
	if err != nil {
		t.Fatalf("failed to read contract file: %v", err)
	}

	var event domain.InventoryReservedEvent
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("failed to unmarshal inventory_failed.json: %v", err)
	}

	if event.Status != "FAILED" {
		t.Errorf("expected status FAILED, got %s", event.Status)
	}

	if event.Reason != "insufficient stock" {
		t.Errorf("expected reason 'insufficient stock', got %s", event.Reason)
	}
}
