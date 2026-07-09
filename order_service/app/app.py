from fastapi import FastAPI

from api.router import router

app = FastAPI(
    title="Order Service",
)
app.include_router(router)
