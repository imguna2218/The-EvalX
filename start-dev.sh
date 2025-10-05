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
echo "🔥 In a new terminal, start the API SERVER:"
echo "   ./target/release/evalx"
echo ""
echo "🔥 In another new terminal, start a WORKER:"
echo "   ./target/release/evalx worker"
echo ""
echo "➡️ You can now send requests to http://localhost:3000"