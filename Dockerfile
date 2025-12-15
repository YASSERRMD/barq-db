# Builder stage
FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy entire workspace
COPY . .

# Build binaries (release mode)
# We build both server and admin CLI
RUN cargo build --workspace --release --bin barq-server --bin barq-admin

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies (OpenSSL/CA certs might be needed depending on deps)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries from builder
COPY --from=builder /app/target/release/barq-server /usr/local/bin/barq-server
COPY --from=builder /app/target/release/barq-admin /usr/local/bin/barq-admin

# Create data directory
RUN mkdir -p /app/data

# Environment variables
ENV BARQ_ADDR=0.0.0.0:8080
ENV BARQ_STORAGE_DIR=/app/data
ENV RUST_LOG=info,barq_api=info,barq_storage=info

# Expose API port
EXPOSE 8080

# Run the server by default
ENTRYPOINT ["barq-server"]
