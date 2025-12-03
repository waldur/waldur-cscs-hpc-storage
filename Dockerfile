# Use to avoid pull rate limit for Docker Hub images
ARG DOCKER_REGISTRY=docker.io/

FROM python:3.11-alpine
LABEL authors="opennode"

COPY . /app

WORKDIR /app

EXPOSE 8080

RUN apk add git

RUN pip install uv --no-cache && uv sync && uv pip install "uvicorn[standard]"

ENV PATH="/app/.venv/bin:$PATH"

CMD ["uvicorn", "waldur_cscs_hpc_storage.waldur_storage_proxy.main:app", "--host", "0.0.0.0", "--port", "8080"]
