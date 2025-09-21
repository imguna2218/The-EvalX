# Build stage - Optimized for Caching
FROM rust:1.85-slim AS builder
WORKDIR /usr/src/evalx

# 1. Copy only the dependency manifests
COPY Cargo.toml Cargo.lock ./

# 2. Build a dummy project to cache dependencies
# This layer only gets rebuilt if Cargo.toml or Cargo.lock changes
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release

# 3. Copy your actual source code
COPY ./src ./src

# 4. Build your application with cached dependencies
RUN touch src/main.rs && \
    cargo build --release

# --- Run Stage ---
FROM debian:bookworm-slim
WORKDIR /app

# Copy the final compiled binary from the builder stage
COPY --from=builder /usr/src/evalx/target/release/evalx /app/evalx

# Expose the port the app runs on
EXPOSE 3000

# The default command to run when the container starts
CMD ["/app/evalx"]