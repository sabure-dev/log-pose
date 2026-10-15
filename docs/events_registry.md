# Event Registry

## Domain: Orders

### `order.created`

- **Exchange:** `logpose_events` (Topic)
- **Routing Key:** `order.created`
- **Publisher:** `order_service`

**Payload (JSON):**
```json
{
  "event_id": "8a0c2049-74d6-4e5a-8b1b-68936efc9df5",
  "event_type": "OrderCreated",
  "order_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "user_id": "123e4567-e89b-12d3-a456-426614174000",
  "total_amount": "3301.00",
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
