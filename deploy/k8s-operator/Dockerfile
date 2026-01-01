# Builder stage
FROM rust:1-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy entire workspace
# Note: This context implies we run `docker build` from the root of the workspace
COPY . .

# Build the operator binary
RUN cargo build --release -p barq-operator

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy binary from builder
COPY --from=builder /app/target/release/barq-operator /usr/local/bin/barq-operator

# Environment variables
ENV RUST_LOG=info

# Run the operator
ENTRYPOINT ["barq-operator"]
