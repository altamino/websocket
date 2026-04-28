FROM astral/uv:python3.14-alpine AS base

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-install-project

RUN adduser -D -u 1000 appuser

COPY . .

RUN uv sync --frozen

RUN mkdir -p /home/appuser/.cache/uv && \
    chown -R 1000:1000 /app /home/appuser/

USER 1000

ENV PATH="/app/.venv/bin:$PATH"

#CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081", "--ws-ping-interval", "60", "--ws-ping-timeout", "120"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8081", "--ws-ping-interval", "0", "--ws-ping-timeout", "0"]
