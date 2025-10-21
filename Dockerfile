FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:0.9.4 /uv /uvx /bin/

ARG APPNAME=datagen

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && useradd -r -m -g appuser appuser && \
    mkdir -p /app/${APPNAME} && chown -R appuser:appuser /app/${APPNAME}
USER appuser
WORKDIR /app/${APPNAME}

ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1

COPY ./uv.lock /app/${APPNAME}/uv.lock
COPY ./pyproject.toml /app/${APPNAME}/pyproject.toml

RUN uv sync --no-dev --no-editable --compile-bytecode --frozen --no-install-project

COPY ./src /app/${APPNAME}/src
COPY ./samples /app/samples

RUN uv sync --no-dev --no-editable --compile-bytecode --frozen

ENV PATH="/app/${APPNAME}/.venv/bin:$PATH"

ENTRYPOINT ["python"]
