FROM astral/uv:python3.14-alpine AS base

WORKDIR /app
COPY . .

RUN uv sync

CMD ["uv", "run", "uvicorn", "main:app", "--host", "127.0.0.1", "--port", "8081"]
