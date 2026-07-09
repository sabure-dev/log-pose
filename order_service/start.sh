#!/bin/bash
set -e
uv run --no-sync alembic upgrade head
uv run --no-sync fastapi run app/main.py --host 0.0.0.0 --port 8000