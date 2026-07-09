from httpx import AsyncClient


async def test_create_order_endpoint(async_client: AsyncClient):
    payload = {
        "items": [
            {
                "product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "quantity": 2,
                "price": "1500.50",
            },
            {
                "product_id": "123e4567-e89b-12d3-a456-426614174000",
                "quantity": 1,
                "price": "300.00",
            },
        ]
    }

    response = await async_client.post("/orders/", json=payload)

    assert response.status_code == 201

    data = response.json()

    assert "id" in data
    assert "created_at" in data
    assert data["status"] == "pending"

    assert data["total_amount"] == "3301.00"

    assert len(data["items"]) == 2
    assert data["items"][0]["quantity"] == 2
