# Runs any pipeline step in a container, the same way on a laptop, in CI,
# or on a container service such as AWS Batch or ECS.
#
#   docker build -t quake-pipeline .
#   docker run --rm -v "$PWD/data:/app/data" quake-pipeline quake-ingest-usgs --incremental

# --- Build stage: build the package as a wheel -----------------------------
FROM python:3.12-slim AS build

WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src/ src/
RUN pip wheel --no-cache-dir --wheel-dir /wheels .

# --- Runtime stage: only the installed package, no build tools -------------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels

# Run as a non-root user. Data is written to /app/data, so mount a volume there
# (or sync it to and from S3) to keep it after the container exits.
RUN useradd --create-home --uid 1000 pipeline
WORKDIR /app
COPY quality/ quality/
RUN mkdir data && chown pipeline:pipeline data
USER pipeline

# No fixed entrypoint: each pipeline step is its own command.
CMD ["quake-ingest-usgs", "--help"]
