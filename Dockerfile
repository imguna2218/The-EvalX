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
RUN apt-get update && apt-get install -y --no-install-recommends \
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
    docbook-xsl \
    systemd \
    systemd-sysv \
    dbus \
    strace \
    && rm -rf /var/lib/apt/lists/*


# ADD: Create Java alternatives to ensure Java is properly linked in /usr/bin
RUN update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-11-openjdk-amd64/bin/java 1 && \
    update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/java-11-openjdk-amd64/bin/javac 1 && \
    update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-21-openjdk-amd64/bin/java 2 && \
    update-alternatives --install /usr/bin/javac javac /usr/lib/jvm/java-21-openjdk-amd64/bin/javac 2

# Verification steps: Ensure toolchains are properly installed and binaries are accessible
RUN gcc --version && \
    g++ --version && \
    javac -version && \
    python3 --version && \
    node --version && \
    which gcc && \
    which g++ && \
    which javac && \
    which python3 && \
    which node && \
    ls -la /usr/lib/gcc && \
    ls -la /usr/lib/jvm && \
    ls -la /usr/lib/nodejs && \
    ls -la /usr/lib/python3 && \
    ls -la /usr/lib/python3.10 && \
    ls -la /usr/lib/x86_64-linux-gnu | head -10

# Build and install isolate using its official 'install' target for a robust setup.
RUN groupadd --system isolate && \
    git clone https://github.com/ioi/isolate.git /tmp/isolate && \
    cd /tmp/isolate && \
    git checkout master && \
    make install && \
    cd / && rm -rf /tmp/isolate && \
    mkdir -p /usr/local/lib/systemd/system

# Create isolate cgroup configuration
RUN mkdir -p /run/isolate && echo "/sys/fs/cgroup/system.slice/isolate.service" > /run/isolate/cgroup


WORKDIR /app

# Copy the final compiled binary from the builder stage
COPY --from=builder /usr/src/evalx/target/release/evalx /app/evalx

# Copy isolate startup script
COPY start-isolate.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-isolate.sh

# Create systemd service files
RUN echo '[Unit]\n\
Description=Isolate sandbox control service\n\
After=systemd-tmpfiles-setup.service\n\
\n\
[Service]\n\
Type=notify\n\
ExecStart=/usr/local/sbin/isolate-cg-keeper\n\
Delegate=yes\n\
RemainAfterExit=yes\n\
User=root\n\
Group=root\n\
\n\
[Install]\n\
WantedBy=multi-user.target' > /usr/local/lib/systemd/system/isolate.service

EXPOSE 3000

ENTRYPOINT ["/usr/local/bin/start-isolate.sh"]
CMD ["/app/evalx"]
