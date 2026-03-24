FROM astral/uv:python3.14-slim AS base

WORKDIR /app
COPY . .

RUN uv sync

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081"]
