FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_SYNC=1

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.6.14 /uv /uvx /bin/

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project

COPY . .

RUN chmod +x start.sh && \
    useradd --uid 10001 --create-home --shell /sbin/nologin appuser && \
    chown -R appuser:appuser /app

USER appuser

CMD ["./start.sh"]
