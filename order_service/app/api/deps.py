import secrets
import uuid
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.interfaces import AbstractUnitOfWork
from app.repositories.uow import SqlAlchemyUnitOfWork
from app.services.order import OrderService

ALLOWED_OPERATOR_ROLES = {"admin", "operator", "warehouse"}


def get_uow(session: AsyncSession = Depends(get_db)) -> AbstractUnitOfWork:
    return SqlAlchemyUnitOfWork(session=session)


def get_order_service(uow: AbstractUnitOfWork = Depends(get_uow)) -> OrderService:
    return OrderService(uow=uow)


def get_current_user_id(
    x_user_id: Annotated[
        uuid.UUID,
        Header(
            alias="X-User-Id",
            description="UUID текущего пользователя",
        ),
    ],
) -> uuid.UUID:
    return x_user_id


def verify_operator_access(
    x_internal_token: Annotated[
        str | None,
        Header(
            alias="X-Internal-Token",
            description="Служебный токен для складских/операторских операций",
        ),
    ] = None,
    x_user_role: Annotated[
        str | None,
        Header(
            alias="X-User-Role",
            description="Роль пользователя из API Gateway (admin, operator, warehouse)",
        ),
    ] = None,
) -> None:
    if x_internal_token and secrets.compare_digest(
        x_internal_token, settings.internal_api_token
    ):
        return
    if x_user_role and x_user_role.strip().lower() in ALLOWED_OPERATOR_ROLES:
        return
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Operator role or valid X-Internal-Token required",
    )


OrderServiceDep = Annotated[OrderService, Depends(get_order_service)]
UserIdDep = Annotated[uuid.UUID, Depends(get_current_user_id)]
OperatorAuthDep = Annotated[None, Depends(verify_operator_access)]
