FROM astral/uv:python3.14-alpine AS base

WORKDIR /app
COPY . .

RUN uv sync

RUN adduser -D -u 1000 appuser && chown -R 1000:1000 /app
USER 1000

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081", "--ws-ping-interval", "30", "--ws-ping-timeout", "120"]
