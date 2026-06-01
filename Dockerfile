# syntax=docker/dockerfile:1
FROM rust:1.96-slim-bookworm AS builder
WORKDIR /app

# Install build dependencies (libssl-dev for OpenSSL, pkg-config)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

# Cache dependencies layer
COPY Cargo.toml Cargo.lock* ./
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release && rm -rf src target/release/deps/polymarket_market_discovery*

# Build real app
COPY . .
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim
WORKDIR /app

# Install runtime deps (libssl, ca-certificates for HTTPS)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/market-discovery /app/
COPY .env.example ./.env.example

EXPOSE 8080

CMD ["./market-discovery"]
