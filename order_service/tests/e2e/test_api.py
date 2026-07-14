import uuid

from httpx import AsyncClient

from app.enums import OrderStatus


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


async def test_create_order_with_invalid_data_returns_422(async_client: AsyncClient):
    empty_items_payload = {"items": []}
    response_empty = await async_client.post("/orders/", json=empty_items_payload)

    assert response_empty.status_code == 422
    assert response_empty.json()["detail"][0]["type"] == "too_short"

    invalid_price_payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 0, "price": "-50.00"}]
    }
    response_invalid = await async_client.post("/orders/", json=invalid_price_payload)

    assert response_invalid.status_code == 422

    errors = response_invalid.json()["detail"]
    assert len(errors) == 2
    error_types = [err["type"] for err in errors]
    assert "greater_than_equal" in error_types


async def test_get_order_list(async_client: AsyncClient):
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "price": "100.00"}]
    }
    await async_client.post("/orders/", json=payload)
    await async_client.post("/orders/", json=payload)

    response = await async_client.get("/orders/?skip=0&limit=10")

    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    assert len(data) >= 2


async def test_get_single_order(async_client: AsyncClient):
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 2, "price": "50.00"}]
    }
    create_resp = await async_client.post("/orders/", json=payload)
    order_id = create_resp.json()["id"]

    get_resp = await async_client.get(f"/orders/{order_id}")

    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == order_id
    assert get_resp.json()["status"] == OrderStatus.PENDING.value


async def test_get_non_existent_order_returns_404(async_client: AsyncClient):
    fake_id = str(uuid.uuid4())

    response = await async_client.get(f"/orders/{fake_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Order not found"


async def test_update_order_status(async_client: AsyncClient):
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "price": "10.00"}]
    }
    create_resp = await async_client.post("/orders/", json=payload)
    order_id = create_resp.json()["id"]

    update_payload = {"status": OrderStatus.RESERVED.value}
    patch_resp = await async_client.patch(
        f"/orders/{order_id}/status", json=update_payload
    )

    assert patch_resp.status_code == 200
    assert patch_resp.json()["status"] == OrderStatus.RESERVED.value


async def test_update_status_with_invalid_enum(async_client: AsyncClient):
    fake_id = str(uuid.uuid4())
    update_payload = {"status": "random_wrong_status"}

    response = await async_client.patch(
        f"/orders/{fake_id}/status", json=update_payload
    )

    assert response.status_code == 422
