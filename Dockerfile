FROM python:3.11-slim
LABEL authors="snowtigersoft"

RUN apt-get update && apt-get -y upgrade && \
    apt-get install -y build-essential libffi-dev libssl-dev locales curl unzip pkg-config && \
    apt-get clean && \
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV INSTALL_PATH=/app PATH="/root/.cargo/bin:$PATH:$INSTALL_PATH" PYTHONPATH="$INSTALL_PATH"

WORKDIR /app

USER root

COPY requirements.txt requirements.txt
RUN mkdir -p /app && \
  pip install pip --upgrade && \
  pip install --no-cache-dir -r requirements.txt && \
  curl -LO https://github.com/HarukaMa/aleo-explorer-rust/archive/refs/heads/master.zip && \
  unzip master.zip && rm master.zip && cd /app/aleo-explorer-rust-master && \
  pip install .

RUN groupadd -r app && useradd -r -g app app

ADD . .

USER app

CMD ["python", "main.py"]