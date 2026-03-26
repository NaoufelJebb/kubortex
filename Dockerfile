# Multi-stage Dockerfile — one target per component.
#
# Build a specific component:
#   docker build --target operator  -t kubortex-operator:latest .
#   docker build --target edge      -t kubortex-edge:latest .
#   docker build --target investigator -t kubortex-investigator:latest .
#   docker build --target remediator -t kubortex-remediator:latest .
#
# Or build all four with Docker Bake:
#   docker buildx bake

# ---------------------------------------------------------------------------
# Stage 1 — dependency installer (shared, heavily cached)
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS deps

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

COPY pyproject.toml uv.lock ./
COPY src/ src/

# ---------------------------------------------------------------------------
# Stage 2 — component-specific installs
# ---------------------------------------------------------------------------

FROM deps AS install-operator
RUN uv sync --frozen --no-dev --extra operator

FROM deps AS install-edge
RUN uv sync --frozen --no-dev --extra edge

FROM deps AS install-investigator
RUN uv sync --frozen --no-dev --extra investigator

FROM deps AS install-remediator
RUN uv sync --frozen --no-dev

# ---------------------------------------------------------------------------
# Stage 3 — minimal runtime base
# ---------------------------------------------------------------------------
FROM python:3.12-slim AS runtime-base

WORKDIR /app

RUN groupadd -r kubortex && useradd -r -g kubortex kubortex \
    && mkdir -p /data/payloads /data/learning /data/checkpoints \
    && chown -R kubortex:kubortex /app /data

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"
ENV PYTHONUNBUFFERED=1

# ---------------------------------------------------------------------------
# Stage 4 — final per-component images
# ---------------------------------------------------------------------------

FROM runtime-base AS operator
COPY --from=install-operator --chown=kubortex:kubortex /app/.venv /app/.venv
COPY --from=install-operator --chown=kubortex:kubortex /app/src /app/src
USER kubortex
CMD ["python", "-m", "kubortex.operator.main"]

FROM runtime-base AS edge
COPY --from=install-edge --chown=kubortex:kubortex /app/.venv /app/.venv
COPY --from=install-edge --chown=kubortex:kubortex /app/src /app/src
USER kubortex
EXPOSE 8000
CMD ["python", "-m", "kubortex.edge.main"]

FROM runtime-base AS investigator
COPY --from=install-investigator --chown=kubortex:kubortex /app/.venv /app/.venv
COPY --from=install-investigator --chown=kubortex:kubortex /app/src /app/src
COPY --chown=kubortex:kubortex skills/ skills/
COPY --chown=kubortex:kubortex runbooks/ runbooks/
USER kubortex
CMD ["python", "-m", "kubortex.investigator.main"]

FROM runtime-base AS remediator
COPY --from=install-remediator --chown=kubortex:kubortex /app/.venv /app/.venv
COPY --from=install-remediator --chown=kubortex:kubortex /app/src /app/src
USER kubortex
CMD ["python", "-m", "kubortex.remediator.main"]
