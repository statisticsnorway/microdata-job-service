# Export Poetry Packages
FROM python:3.13-bookworm AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.7.1 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1

# Prepend poetry to path
ENV PATH="$POETRY_HOME/bin:$PATH"

# Install tools
RUN apt-get update \
    && apt-get install -y  --no-install-recommends \
    ca-certificates \
    curl \
    build-essential \
    python3-distutils \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

WORKDIR /app
COPY poetry.lock pyproject.toml /app/

# Install poetry and export dependencies to requirements yaml
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN curl --proto '=https' --tlsv1.2 -sSL https://install.python-poetry.org | python3 - --version "$POETRY_VERSION"

RUN poetry export > requirements.txt
RUN pip install -r requirements.txt --target=/app/dependencies

# Create user
RUN groupadd --gid 180291 microdata \
    && useradd --uid 180291 --gid microdata microdata


# Production image
FROM ghcr.io/statisticsnorway/distroless-python3.13
ARG COMMIT_ID
ENV COMMIT_ID=$COMMIT_ID

WORKDIR /app
COPY job_service job_service
#To use application version in logs
COPY --from=builder /app/pyproject.toml pyproject.toml
COPY --from=builder /app/dependencies /app/dependencies
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group

ENV PYTHONPATH "${PYTHONPATH}:/app:/app/dependencies"

# Change to our non-root user
USER microdata
CMD ["-m", "gunicorn", "-k", "uvicorn.workers.UvicornWorker", "--logger-class", "job_service.config.gunicorn.CustomLogger", "job_service.app:app", "--preload", "--workers", "1", "--bind", "0.0.0.0:8000","--limit-request-line", "8190"] 


