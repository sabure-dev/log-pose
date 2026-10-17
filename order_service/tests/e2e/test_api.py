import json
import uuid
from pathlib import Path

from httpx import AsyncClient
from sqlalchemy import text

from app.enums import OrderStatus
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService


async def test_healthcheck_endpoint(async_client: AsyncClient):
    response = await async_client.get("/orders/healthcheck")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


async def test_create_order_endpoint(async_client: AsyncClient, db_session):
    custom_user_id = str(uuid.uuid4())
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
        ],
        "shipping_address": "Main Street 10",
    }

    response = await async_client.post(
        "/orders/", json=payload, headers={"X-User-Id": custom_user_id}
    )

    assert response.status_code == 201

    data = response.json()

    assert "id" in data
    assert "created_at" in data
    assert data["user_id"] == custom_user_id
    assert data["status"] == "pending"
    assert data["total_amount"] == "3301.00"
    assert data["shipping_address"] == "Main Street 10"

    assert len(data["items"]) == 2
    assert data["items"][0]["quantity"] == 2

    result = await db_session.execute(text("SELECT * FROM outbox_events"))
    events = result.mappings().all()

    assert len(events) == 1
    assert events[0]["topic"] == "order.created"
    assert not events[0]["processed"]

    payload_data = json.loads(events[0]["payload"])
    assert payload_data["total_amount"] == "3301.00"
    assert payload_data["user_id"] == custom_user_id
    assert payload_data["shipping_address"] == "Main Street 10"
    assert "event_id" in payload_data
    assert "occurred_at" in payload_data
    assert "correlation_id" in payload_data


async def test_create_order_idempotency_key(async_client: AsyncClient, db_session):
    user_id = str(uuid.uuid4())
    payload = {
        "items": [
            {
                "product_id": str(uuid.uuid4()),
                "quantity": 1,
                "price": "150.00",
            }
        ],
        "shipping_address": "Idempotent Way 1",
    }
    headers = {"X-User-Id": user_id, "Idempotency-Key": "order-req-001"}

    resp1 = await async_client.post("/orders/", json=payload, headers=headers)
    resp2 = await async_client.post("/orders/", json=payload, headers=headers)

    assert resp1.status_code == 201
    assert resp2.status_code == 201
    assert resp1.json()["id"] == resp2.json()["id"]

    result = await db_session.execute(text("SELECT * FROM outbox_events"))
    assert len(result.mappings().all()) == 1


async def test_missing_user_id_header_returns_422(async_client: AsyncClient):
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "price": "10.00"}]
    }
    response = await async_client.post("/orders/", json=payload)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["header", "X-User-Id"]


async def test_create_order_with_invalid_data_returns_422(async_client: AsyncClient):
    user_id = str(uuid.uuid4())
    empty_items_payload = {"items": []}
    response_empty = await async_client.post(
        "/orders/", json=empty_items_payload, headers={"X-User-Id": user_id}
    )

    assert response_empty.status_code == 422
    assert response_empty.json()["detail"][0]["type"] == "too_short"

    invalid_price_payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 0, "price": "-50.00"}]
    }
    response_invalid = await async_client.post(
        "/orders/", json=invalid_price_payload, headers={"X-User-Id": user_id}
    )

    assert response_invalid.status_code == 422

    errors = response_invalid.json()["detail"]
    assert len(errors) == 2
    error_types = [err["type"] for err in errors]
    assert "greater_than_equal" in error_types
    assert "greater_than" in error_types


async def test_get_order_list_scoped_to_user(async_client: AsyncClient):
    user_id_a = str(uuid.uuid4())
    user_id_b = str(uuid.uuid4())
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "price": "100.00"}]
    }
    await async_client.post("/orders/", json=payload, headers={"X-User-Id": user_id_a})
    await async_client.post("/orders/", json=payload, headers={"X-User-Id": user_id_a})
    await async_client.post("/orders/", json=payload, headers={"X-User-Id": user_id_b})

    response_a = await async_client.get(
        "/orders/?skip=0&limit=10", headers={"X-User-Id": user_id_a}
    )
    assert response_a.status_code == 200
    data_a = response_a.json()
    assert len(data_a) == 2
    assert all(order["user_id"] == user_id_a for order in data_a)

    response_b = await async_client.get(
        "/orders/?skip=0&limit=10", headers={"X-User-Id": user_id_b}
    )
    assert response_b.status_code == 200
    data_b = response_b.json()
    assert len(data_b) == 1
    assert data_b[0]["user_id"] == user_id_b


async def test_get_single_order_scoped_to_user(async_client: AsyncClient):
    user_id_a = str(uuid.uuid4())
    user_id_b = str(uuid.uuid4())
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 2, "price": "50.00"}]
    }
    create_resp = await async_client.post(
        "/orders/", json=payload, headers={"X-User-Id": user_id_a}
    )
    order_id = create_resp.json()["id"]

    get_resp_a = await async_client.get(
        f"/orders/{order_id}", headers={"X-User-Id": user_id_a}
    )
    assert get_resp_a.status_code == 200
    assert get_resp_a.json()["id"] == order_id
    assert get_resp_a.json()["status"] == OrderStatus.PENDING.value

    get_resp_b = await async_client.get(
        f"/orders/{order_id}", headers={"X-User-Id": user_id_b}
    )
    assert get_resp_b.status_code == 404


async def test_get_non_existent_order_returns_404(async_client: AsyncClient):
    fake_id = str(uuid.uuid4())
    user_id = str(uuid.uuid4())
    response = await async_client.get(
        f"/orders/{fake_id}", headers={"X-User-Id": user_id}
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Order not found"


async def test_cancel_order_endpoint(async_client: AsyncClient):
    user_id = str(uuid.uuid4())
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "price": "10.00"}]
    }
    create_resp = await async_client.post(
        "/orders/", json=payload, headers={"X-User-Id": user_id}
    )
    order_id = create_resp.json()["id"]

    cancel_resp = await async_client.post(
        f"/orders/{order_id}/cancel", headers={"X-User-Id": user_id}
    )

    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == OrderStatus.CANCELLED.value
    assert cancel_resp.json()["cancellation_reason"] == "cancelled by user"


async def test_ship_and_complete_order_endpoints(async_client: AsyncClient, db_session):
    user_id = str(uuid.uuid4())
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 2, "price": "50.00"}]
    }
    create_resp = await async_client.post(
        "/orders/", json=payload, headers={"X-User-Id": user_id}
    )
    order_id = uuid.UUID(create_resp.json()["id"])

    # Cannot ship while still PENDING
    ship_early = await async_client.post(
        f"/orders/{order_id}/ship", headers={"X-User-Id": user_id}
    )
    assert ship_early.status_code == 409

    # Transition to RESERVED (simulating inventory.reserved consumer)
    uow = SqlAlchemyUnitOfWork(session=db_session)
    service = OrderService(uow=uow)
    await service.update_order_status(order_id, OrderStatus.RESERVED)

    ship_resp = await async_client.post(
        f"/orders/{order_id}/ship", headers={"X-User-Id": user_id}
    )
    assert ship_resp.status_code == 200
    assert ship_resp.json()["status"] == OrderStatus.READY_FOR_SHIPPING.value

    complete_resp = await async_client.post(
        f"/orders/{order_id}/complete", headers={"X-User-Id": user_id}
    )
    assert complete_resp.status_code == 200
    assert complete_resp.json()["status"] == OrderStatus.COMPLETED.value


async def test_cancel_other_users_order_returns_404(async_client: AsyncClient):
    user_id_a = str(uuid.uuid4())
    user_id_b = str(uuid.uuid4())
    payload = {
        "items": [{"product_id": str(uuid.uuid4()), "quantity": 1, "price": "10.00"}]
    }
    create_resp = await async_client.post(
        "/orders/", json=payload, headers={"X-User-Id": user_id_a}
    )
    order_id = create_resp.json()["id"]

    cancel_resp = await async_client.post(
        f"/orders/{order_id}/cancel", headers={"X-User-Id": user_id_b}
    )
    assert cancel_resp.status_code == 404


async def test_emitted_outbox_payloads_match_contract_schemas(
    async_client: AsyncClient, db_session
):
    contracts_dir = Path(__file__).parent.parent.parent.parent / "docs" / "contracts"
    with open(contracts_dir / "order_created.json") as f:
        created_contract = json.load(f)
    with open(contracts_dir / "order_cancelled.json") as f:
        cancelled_contract = json.load(f)
    with open(contracts_dir / "order_completed.json") as f:
        completed_contract = json.load(f)

    user_id = str(uuid.uuid4())
    payload = {
        "items": [
            {
                "product_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                "quantity": 2,
                "price": "1500.50",
            }
        ],
        "shipping_address": "Main Street 100, Apt 4B",
    }
    resp = await async_client.post(
        "/orders/", json=payload, headers={"X-User-Id": user_id}
    )
    order_id = uuid.UUID(resp.json()["id"])

    # Transition to RESERVED -> READY_FOR_SHIPPING -> COMPLETED
    uow = SqlAlchemyUnitOfWork(session=db_session)
    service = OrderService(uow=uow)
    await service.update_order_status(order_id, OrderStatus.RESERVED)
    await async_client.post(f"/orders/{order_id}/ship", headers={"X-User-Id": user_id})
    await async_client.post(
        f"/orders/{order_id}/complete", headers={"X-User-Id": user_id}
    )

    # Create a second order and cancel it
    resp2 = await async_client.post(
        "/orders/", json=payload, headers={"X-User-Id": user_id}
    )
    order2_id = resp2.json()["id"]
    await async_client.post(
        f"/orders/{order2_id}/cancel", headers={"X-User-Id": user_id}
    )

    result = await db_session.execute(
        text("SELECT topic, payload FROM outbox_events ORDER BY created_at ASC")
    )
    rows = result.mappings().all()
    by_topic = {row["topic"]: json.loads(row["payload"]) for row in rows}

    assert set(by_topic["order.created"].keys()) == set(created_contract.keys())
    assert set(by_topic["order.created"]["items"][0].keys()) == set(
        created_contract["items"][0].keys()
    )
    assert by_topic["order.created"]["total_amount"] == "3001.00"

    assert set(by_topic["order.cancelled"].keys()) == set(cancelled_contract.keys())
    assert set(by_topic["order.cancelled"]["items"][0].keys()) == set(
        cancelled_contract["items"][0].keys()
    )

    assert set(by_topic["order.completed"].keys()) == set(completed_contract.keys())
    assert set(by_topic["order.completed"]["items"][0].keys()) == set(
        completed_contract["items"][0].keys()
    )
