from fastapi import FastAPI

from app.api.router import router

app = FastAPI(
    title="Order Service",
)
app.include_router(router)
