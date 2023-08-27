FROM python:3.11-slim as builder
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Installs Aleo module build dependencies
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
    git \
    curl \
    build-essential \
    pkg-config \
    libssl-dev \ 
    ;

# Installs Rust compiler
RUN set -eux; \
    curl https://sh.rustup.rs | bash -s -- -y
ENV PATH="${PATH}:/root/.cargo/bin"

# Builds aleo rust module wheel
RUN set -eux; \
    pip install setuptools-rust --no-cache-dir; \
    git clone https://github.com/HarukaMa/aleo-explorer-rust.git ; \
    pip wheel -w /dist/ ./aleo-explorer-rust

# Clones repo
RUN set -eux; \
    git clone https://github.com/HarukaMa/aleo-explorer.git /app/

# Builds requirements wheels
RUN set -eux; \
    pip wheel -w /dist/  -r /app/requirements.txt 


FROM python:3.11-slim as runtime
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY --from=builder /app/ /app/

# Installs wheels from builder
RUN --mount=source=/dist/,target=/dist/,from=builder \ 
    pip install --no-cache-dir --no-index /dist/*.whl

WORKDIR /app/
RUN adduser -u 5678 --disabled-password --gecos "" appuser && chown -R appuser /app
USER appuser

CMD ["python", "-m", "main"]
