FROM python:3.11-slim AS builder

ENV APP_DIR="/app"
ENV PATH="${PATH}:/root/.cargo/bin"
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    pkg-config \
    libssl-dev; \
    # Installs Rust compiler
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y; \
    pip install -U pip; pip install uv; \
    # Setup aleo-explorer
    git clone https://github.com/HarukaMa/aleo-explorer.git ${APP_DIR}; \
    cd ${APP_DIR}; uv sync --no-editable 

FROM python:3.11-slim

COPY --from=builder /app/.venv /app/

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PATH="/app/bin:${PATH}"
WORKDIR /app/
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

CMD ["python3", "-m", "main"]
