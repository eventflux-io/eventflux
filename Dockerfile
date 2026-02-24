# ==============================================================================
# EventFlux Engine - Production Docker Image
# ==============================================================================
# Multi-stage build following OCI and Docker best practices
#
# Build (local tag is arbitrary):
#   docker build -t eventflux:local .
#
# Run (executes a `.eventflux` file; this is a CLI container, not a web service):
#   docker run --rm -v $(pwd)/query.eventflux:/app/query.eventflux:ro \
#     --network host eventflux:local /app/query.eventflux
#
# Official pre-built image:
#   ghcr.io/eventflux-io/eventflux:latest
#
# ==============================================================================

# ------------------------------------------------------------------------------
# Build Arguments
# ------------------------------------------------------------------------------
ARG RUST_VERSION=1.85
ARG DEBIAN_VERSION=bookworm

# ------------------------------------------------------------------------------
# Stage 1: Builder
# ------------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    libprotobuf-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy dependency files first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY vendor/ vendor/
COPY build.rs ./
COPY proto/ proto/
COPY benches/ benches/
COPY tests/custom_dyn_ext/ tests/custom_dyn_ext/

# Create dummy source for dependency caching
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/bin/run_eventflux.rs && \
    echo "pub fn lib() {}" > src/lib.rs

# Build dependencies only (cached layer)
RUN cargo build --release --locked --bin run_eventflux

# Remove dummy files
RUN rm -rf src

# Copy actual source code
COPY src/ src/

# Docker `COPY` preserves mtimes from the build context; if they are older than
# the previously-built placeholder artifacts, Cargo can incorrectly treat the
# crate as up-to-date. Touch sources to force a correct rebuild.
RUN find src -type f -exec touch {} +

# Build release binary with optimizations
RUN cargo build --release --locked --bin run_eventflux && \
    strip /build/target/release/run_eventflux

# Verify binary
RUN test -x /build/target/release/run_eventflux
RUN /build/target/release/run_eventflux --help | grep -q "Usage:"

# ------------------------------------------------------------------------------
# Stage 2: Runtime
# ------------------------------------------------------------------------------
FROM debian:${DEBIAN_VERSION}-slim AS runtime

# OCI Image Labels (https://github.com/opencontainers/image-spec/blob/main/annotations.md)
LABEL org.opencontainers.image.title="EventFlux Engine" \
      org.opencontainers.image.description="High-performance Complex Event Processing engine for real-time streaming analytics" \
      org.opencontainers.image.vendor="EventFlux" \
      org.opencontainers.image.source="https://github.com/eventflux-io/eventflux" \
      org.opencontainers.image.documentation="https://eventflux.io/docs" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

# Install runtime dependencies and tini for proper signal handling
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    tini \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user for security
RUN groupadd --gid 1000 eventflux && \
    useradd --uid 1000 --gid eventflux --shell /bin/bash --create-home eventflux

# Create application directories
RUN mkdir -p /app/queries /app/data /app/config && \
    chown -R eventflux:eventflux /app

WORKDIR /app

# Copy binary from builder
COPY --from=builder --chown=eventflux:eventflux /build/target/release/run_eventflux /usr/local/bin/run

# Switch to non-root user
USER eventflux

# Environment configuration
ENV RUST_LOG=info \
    RUST_BACKTRACE=1

# Use tini as init system for proper signal handling
ENTRYPOINT ["/usr/bin/tini", "--", "run"]

# Default command shows help
CMD ["--help"]

# Signal for graceful shutdown
STOPSIGNAL SIGTERM

# ==============================================================================
# Usage Examples:
# ==============================================================================
#
# 1. Run with a query file:
#    docker run --rm \
#      -v $(pwd)/my_query.eventflux:/app/query.eventflux:ro \
#      --network host \
#      ghcr.io/eventflux-io/eventflux:latest /app/query.eventflux
#
# 2. Run with persistence:
#    docker run --rm \
#      -v $(pwd)/queries:/app/queries:ro \
#      -v $(pwd)/data:/app/data \
#      --network host \
#      ghcr.io/eventflux-io/eventflux:latest /app/queries/app.eventflux \
#        --persistence-dir /app/data/snapshots
#
# 3. Run with config file:
#    docker run --rm \
#      -v $(pwd)/queries:/app/queries:ro \
#      -v $(pwd)/config:/app/config:ro \
#      --network host \
#      ghcr.io/eventflux-io/eventflux:latest /app/queries/app.eventflux \
#        --config /app/config/eventflux.yaml
#
# 4. Debug with shell access:
#    docker run --rm -it \
#      --entrypoint /bin/sh \
#      ghcr.io/eventflux-io/eventflux:latest
#
# ==============================================================================
