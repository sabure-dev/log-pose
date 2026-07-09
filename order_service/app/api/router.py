import uuid

from app.api.deps import get_order_service
from app.api.schemas import OrderRead, OrderCreate
from app.services.order import OrderService
from fastapi import APIRouter, status, Depends

router = APIRouter(prefix="/orders", tags=["Orders"])


@router.get("/healthcheck", status_code=status.HTTP_200_OK)
async def healthcheck():
    return {"status": "ok"}


@router.post("/", response_model=OrderRead, status_code=status.HTTP_201_CREATED)
async def create_order(
    order_in: OrderCreate, service: OrderService = Depends(get_order_service)
) -> OrderRead:
    current_user_id = uuid.uuid4()

    return await service.create_new_order(current_user_id, order_in)
