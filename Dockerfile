# syntax=docker/dockerfile:1
FROM alpine:3.24.4 AS base
ENV APP_USER=worker \
    APP_USER_ID=5556 \
    WORK_DIR=/app \
    VIRTUAL_ENV=/opt/venv \
    PYTHON_VERSION=3.14 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Minimal Python from Alpine's repos (shared by all stages), pinned via apk's
# fuzzy match ("~="): 3.14 -> any 3.14.x, 3.14.2 -> 3.14.2-rN only.
# PYTHON_VERSION is bumped to the latest patch by the "Update Python version" workflow.
# The venv symlinks to /usr/bin/python3, so every stage needs this same package.
RUN apk --no-cache upgrade \
    && apk --no-cache add "python3~=${PYTHON_VERSION}"

# ============================================
# Stage 1: Build stage
# ============================================
FROM base AS builder
ARG TARGETPLATFORM

# Install build dependencies. There are no prebuilt musl wheels for some Rust
# extensions (e.g. pydantic-core) on 32-bit ARM, so those need a Rust toolchain.
RUN if [ "$TARGETPLATFORM" = "linux/arm/v6" ] || [ "$TARGETPLATFORM" = "linux/arm/v7" ]; then \
        BUILD_DEPS="cargo rust"; \
    fi \
    && apk add --no-cache py3-pip ${BUILD_DEPS}

# Create virtual environment WITHOUT pip/setuptools. Packages are installed from
# the builder's system pip via `pip --python`, so the venv never contains pip.
RUN python3 -m venv --without-pip $VIRTUAL_ENV

# Install the runtime dependencies first, so this layer stays cached as long as
# requirements.txt does not change.
COPY requirements.txt ${WORK_DIR}/
WORKDIR ${WORK_DIR}
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip --python $VIRTUAL_ENV/bin/python install -r requirements.txt

# Application code
COPY . ${WORK_DIR}

# ============================================
# Stage 2: Test stage
# ============================================
FROM builder AS test

# Test the code against the venv in a copy, so pytest never lands in the
# production image. Runs once per target platform.
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    cp -a $VIRTUAL_ENV /tmp/venv-test \
    && pip --python /tmp/venv-test/bin/python install --group test \
    && /tmp/venv-test/bin/python -m pytest --exitfirst \
    && touch /tmp/tests-passed

# ============================================
# Stage 3: Production stage
# ============================================
FROM base AS prod
LABEL maintainer="Patrick Baus <patrick.baus@physik.tu-darmstadt.de>" \
      description="Kraken sensor data aggregator."

# Add an unprivileged user with a fixed UID/GID (useful for mounted secrets)
RUN addgroup -g ${APP_USER_ID} ${APP_USER} \
    && adduser -D -u ${APP_USER_ID} -G ${APP_USER} ${APP_USER}

# Only build the production image if the tests passed
COPY --from=test /tmp/tests-passed /tmp/tests-passed
# Copy virtual environment from builder
COPY --from=builder $VIRTUAL_ENV $VIRTUAL_ENV
# Copy application code (without tests and repository metadata)
COPY --chown=${APP_USER}:${APP_USER} --from=builder \
     --exclude=tests --exclude=.git --exclude=.github \
     ${WORK_DIR} ${WORK_DIR}
WORKDIR ${WORK_DIR}

# Switch to non-root user
USER ${APP_USER}

CMD ["python3", "-OO", "kraken_ethernet.py"]

# ============================================
# Stage 4: Development builder stage
# ============================================
FROM builder AS builder_dev

RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    apk add --no-cache git \
    && pip --python $VIRTUAL_ENV/bin/python install --group dev --group test

# ============================================
# Stage 5: Development stage
# ============================================
FROM base AS dev

RUN apk add --no-cache git \
    && addgroup -g ${APP_USER_ID} ${APP_USER} \
    && adduser -D -u ${APP_USER_ID} -G ${APP_USER} ${APP_USER}

# Copy virtual environment from builder
COPY --from=builder_dev $VIRTUAL_ENV $VIRTUAL_ENV
# Copy application code
COPY --chown=${APP_USER}:${APP_USER} --from=builder_dev ${WORK_DIR} ${WORK_DIR}
WORKDIR ${WORK_DIR}

# Switch to non-root user
USER ${APP_USER}

CMD ["python3", "kraken_ethernet.py"]

# ============================================
# Default target: production
# ============================================
FROM prod
