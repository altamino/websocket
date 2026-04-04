FROM astral/uv:python3.14-alpine AS base

WORKDIR /app
COPY . .

RUN useradd -U -u 1000 appuser
RUN mkdir -p /home/appuser/.cache/uv
RUN chown -R 1000:1000 /app /home/appuser/
USER 1000

RUN uv sync

CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081", "--ws-ping-interval", "60", "--ws-ping-timeout", "120"]
