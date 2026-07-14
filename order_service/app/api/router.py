import uuid
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from app.api.deps import OrderServiceDep
from app.api.schemas import OrderCreate, OrderRead, OrderStatusUpdate

router = APIRouter(prefix="/orders", tags=["Orders"])


@router.get("/healthcheck", status_code=status.HTTP_200_OK)
async def healthcheck():
    return {"status": "ok"}


@router.post("/", response_model=OrderRead, status_code=status.HTTP_201_CREATED)
async def create_order(order_in: OrderCreate, service: OrderServiceDep) -> OrderRead:
    current_user_id = uuid.uuid4()
    return await service.create_new_order(current_user_id, order_in)


@router.get("/", response_model=list[OrderRead], status_code=status.HTTP_200_OK)
async def list_orders(
    service: OrderServiceDep,
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
):
    return await service.get_orders(skip, limit)


@router.get("/{order_id}", response_model=OrderRead, status_code=status.HTTP_200_OK)
async def get_order(
    order_id: uuid.UUID,
    service: OrderServiceDep,
):
    order = await service.get_order(order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
        )
    return order


@router.patch(
    "/{order_id}/status", response_model=OrderRead, status_code=status.HTTP_200_OK
)
async def update_order_status(
    order_id: uuid.UUID,
    update_data: OrderStatusUpdate,
    service: OrderServiceDep,
):
    order = await service.update_order_status(order_id, update_data.status)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Order not found"
        )
    return order
