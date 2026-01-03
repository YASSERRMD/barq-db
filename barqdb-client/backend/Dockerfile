# Backend Dockerfile
FROM rust:1-bookworm AS builder

WORKDIR /app

# Install protobuf
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

COPY . .
RUN cargo build --release

# Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/barqdb-client-backend /usr/local/bin/

ENV BARQ_GRPC_ADDR=http://barq-db:50051
EXPOSE 3001

CMD ["barqdb-client-backend"]
