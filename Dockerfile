# Build stage
FROM rust:1.85-slim AS builder
WORKDIR /usr/src/evalx
COPY . .
RUN cargo build --release

# Run stage
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /usr/src/evalx/target/release/evalx /app/evalx
EXPOSE 3000
CMD ["/app/evalx"]