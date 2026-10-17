import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import OperatorAuthDep, OrderServiceDep, UserIdDep
from app.database import get_db
from app.domain import NewOrderItem
from app.exceptions import IdempotencyConflict, InvalidTransition, OrderNotFound
from app.schemas import OrderCreate, OrderRead

router = APIRouter(prefix="/orders", tags=["Orders"])


@router.get("/healthcheck", status_code=status.HTTP_200_OK)
async def healthcheck(db: AsyncSession = Depends(get_db)):
    try:
        await db.execute(text("SELECT 1"))
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc
    return {"status": "ok"}


@router.post("/", response_model=OrderRead, status_code=status.HTTP_201_CREATED)
async def create_order(
    order_in: OrderCreate,
    service: OrderServiceDep,
    current_user_id: UserIdDep,
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            max_length=255,
            description="Optional idempotency key to prevent duplicate order creation",
        ),
    ] = None,
) -> OrderRead:
    domain_items = [
        NewOrderItem(
            product_id=item.product_id,
            quantity=item.quantity,
            price=item.price,
        )
        for item in order_in.items
    ]
    try:
        order = await service.create_new_order(
            current_user_id,
            domain_items,
            order_in.shipping_address,
            idempotency_key=idempotency_key,
        )
    except IdempotencyConflict as err:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(err)
        ) from err
    return OrderRead.model_validate(order)


@router.get("/", response_model=list[OrderRead], status_code=status.HTTP_200_OK)
async def list_orders(
    service: OrderServiceDep,
    current_user_id: UserIdDep,
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
):
    orders = await service.get_orders(current_user_id, skip, limit)
    return [OrderRead.model_validate(o) for o in orders]


@router.get("/{order_id}", response_model=OrderRead, status_code=status.HTTP_200_OK)
async def get_order(
    order_id: uuid.UUID,
    service: OrderServiceDep,
    current_user_id: UserIdDep,
):
    order = await service.get_order(order_id, current_user_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
        )
    return OrderRead.model_validate(order)


@router.post(
    "/{order_id}/cancel", response_model=OrderRead, status_code=status.HTTP_200_OK
)
async def cancel_order(
    order_id: uuid.UUID,
    service: OrderServiceDep,
    current_user_id: UserIdDep,
):
    try:
        order = await service.cancel_order(order_id, current_user_id)
        return OrderRead.model_validate(order)
    except OrderNotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
        )
    except InvalidTransition as err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(err))


@router.post(
    "/{order_id}/ship", response_model=OrderRead, status_code=status.HTTP_200_OK
)
async def ship_order(
    order_id: uuid.UUID,
    service: OrderServiceDep,
    _operator: OperatorAuthDep,
    x_user_id: Annotated[
        uuid.UUID | None,
        Header(alias="X-User-Id", description="Optional owner UUID filter"),
    ] = None,
):
    try:
        order = await service.ship_order(order_id, user_id=x_user_id)
        return OrderRead.model_validate(order)
    except OrderNotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
        )
    except InvalidTransition as err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(err))


@router.post(
    "/{order_id}/complete", response_model=OrderRead, status_code=status.HTTP_200_OK
)
async def complete_order(
    order_id: uuid.UUID,
    service: OrderServiceDep,
    _operator: OperatorAuthDep,
    x_user_id: Annotated[
        uuid.UUID | None,
        Header(alias="X-User-Id", description="Optional owner UUID filter"),
    ] = None,
):
    try:
        order = await service.complete_order(order_id, user_id=x_user_id)
        return OrderRead.model_validate(order)
    except OrderNotFound:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
        )
    except InvalidTransition as err:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(err))
