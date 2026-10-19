import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from typing import Sequence

import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool, StaticPool

from app.api.deps import get_inventory_client
from app.database import Base, get_db
from app.domain import NewOrderItem, StockAvailabilityResult, StockItemCheckResult
from app.interfaces import AbstractInventoryClient
from app.main import app

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "sqlite+aiosqlite:///:memory:")


class MockEligibleInventoryClient(AbstractInventoryClient):
    async def check_stock_availability(
        self, items: Sequence[NewOrderItem]
    ) -> StockAvailabilityResult:
        return StockAvailabilityResult(
            eligible=True,
            overall_status="OK",
            reason=None,
            items=[
                StockItemCheckResult(
                    product_id=it.product_id,
                    status="OK",
                    catalog_price=f"{it.price:.2f}",
                    available_stock=9999,
                    reason=None,
                )
                for it in items
            ],
        )


if TEST_DATABASE_URL.startswith("sqlite"):
    engine_test = create_async_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
else:
    engine_test = create_async_engine(
        TEST_DATABASE_URL,
        poolclass=NullPool,
    )

TestingSessionLocal = async_sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine_test,
    expire_on_commit=False,
)


@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    async with engine_test.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    yield

    async with engine_test.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)


@pytest_asyncio.fixture
async def db_session():
    async with TestingSessionLocal() as session:
        yield session


@pytest_asyncio.fixture
async def async_client(db_session):
    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_inventory_client] = lambda: (
        MockEligibleInventoryClient()
    )

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as ac:
        yield ac

    app.dependency_overrides.clear()
