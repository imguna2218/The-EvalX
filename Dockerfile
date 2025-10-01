# Build stage - Optimized for Caching Rust compilation
FROM rust:1.85-slim AS builder
WORKDIR /usr/src/evalx

# Copy only the dependency manifests
COPY Cargo.toml Cargo.lock ./

# Build a dummy project to cache Rust dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release

# Copy your actual source code
COPY ./src ./src

# Build your application with cached dependencies
RUN touch src/main.rs && \
    cargo build --release

# --- Final Run Stage ---
# Using Ubuntu 22.04 LTS for its broad package support.
FROM ubuntu:22.04

# Prevent apt-get from asking interactive questions
ENV DEBIAN_FRONTEND=noninteractive

# Install all runtime dependencies for languages, and build dependencies for isolate
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    g++ \
    gcc \
    git \
    libcap-dev \
    libseccomp-dev \
    libsystemd-dev \
    nodejs \
    openjdk-11-jdk-headless \
    openjdk-21-jdk-headless \
    pkg-config \
    python3 \
    asciidoc \
    libxml2-utils \
    docbook-xml \
    docbook-xsl && \
    rm -rf /var/lib/apt/lists/*

# ADD: Create Java alternatives to ensure Java is properly linked in /usr/bin
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-11-openjdk-amd64/bin/java 1 && \
    update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/java-11-openjdk-amd64/bin/javac 1 && \
    update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-21-openjdk-amd64/bin/java 2 && \
    update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/java-21-openjdk-amd64/bin/javac 2

# Build and install isolate using its official 'install' target for a robust setup.
RUN groupadd --system isolate && \
    git clone https://github.com/ioi/isolate.git /tmp/isolate && \
    cd /tmp/isolate && \
    # The default branch for this repository is 'master', not 'main'.
    git checkout master && \
    make install && \
    cd / && \
    rm -rf /tmp/isolate

WORKDIR /app

# Copy the final compiled binary from the builder stage
COPY --from=builder /usr/src/evalx/target/release/evalx /app/evalx

# Expose the port the app runs on
EXPOSE 3000

# The default command to run when the container starts
CMD ["/app/evalx"]