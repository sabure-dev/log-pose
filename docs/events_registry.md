# Event Registry

## Domain: Orders

### `order.created`

- **Exchange:** `logpose_events` (Topic)
- **Routing Key:** `order.created`
- **Publisher:** `order_service`
- **Consumer:** `inventory_service`

**Payload (JSON):**
```json
{
  "event_id": "8a0c2049-74d6-4e5a-8b1b-68936efc9df5",
  "event_type": "OrderCreated",
  "order_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "user_id": "123e4567-e89b-12d3-a456-426614174000",
  "total_amount": "3301.00",
  "shipping_address": "Main Street 100, Apt 4B",
  "items": [
    {
      "product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
      "quantity": 2,
      "price": "1500.50"
    }
  ],
  "occurred_at": "2026-10-15T22:30:00.000000+00:00"
}
```

### `order.cancelled`

- **Exchange:** `logpose_events` (Topic)
- **Routing Key:** `order.cancelled`
- **Publisher:** `order_service`
- **Consumer:** `inventory_service`

**Payload (JSON):**
```json
{
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
}
```

## Domain: Inventory

### `inventory.reserved`

- **Exchange:** `logpose_events` (Topic)
- **Routing Key:** `inventory.reserved`
- **Publisher:** `inventory_service`
- **Consumer:** `order_service`

**Payload (JSON):**
```json
{
  "event_id": "1fa85f64-5717-4562-b3fc-2c963f66afa6",
  "event_type": "InventoryReservationResult",
  "order_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "RESERVED"
}
```

### `inventory.failed`

- **Exchange:** `logpose_events` (Topic)
- **Routing Key:** `inventory.failed`
- **Publisher:** `inventory_service`
- **Consumer:** `order_service`

**Payload (JSON):**
```json
{
  "event_id": "2fa85f64-5717-4562-b3fc-2c963f66afa6",
  "event_type": "InventoryReservationResult",
  "order_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "status": "FAILED",
  "reason": "insufficient stock"
}
```
