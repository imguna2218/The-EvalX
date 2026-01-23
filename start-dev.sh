#!/bin/bash
set -e

echo "--- Stopping and removing old Redis container... ---"
# The '|| true' prevents an error if the container doesn't exist
docker compose down --remove-orphans || true

echo "--- Starting Redis service in Docker... ---"
docker compose up -d

echo ""
echo "--- Building Rust binary for host execution... ---"
cargo build --release

echo ""
echo "✅ Setup Complete!"
echo "   - Redis is running in Docker and available at localhost:6379"
echo ""
echo "🔥 In a first new terminal, start the API SERVER:"
echo "   ./target/release/evalx"
echo ""
echo "🔥 In the second new terminal, start worker : "
echo "   ./target/release/evalx worker"
echo ""
echo "➡️   You can now send requests to http://localhost:3000"