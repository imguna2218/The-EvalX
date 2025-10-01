#!/bin/bash

# A robust script to build and start the development environment for evalX.
# It cleans up the environment, stops on any error, and provides clear feedback.

# Exit immediately if a command exits with a non-zero status.
set -e

# NOTE: The aggressive docker restart command has been removed to prevent network instability.
# 'docker compose down' is sufficient for ensuring a clean state.

echo ""
echo "--- Stopping any existing evalX containers... ---"
# This command will now run against a healthy daemon.
docker compose down --remove-orphans

echo ""
echo "--- Building fresh images for all services... ---"
docker compose build

echo ""
echo "--- Starting all services in detached mode... ---"
# This will start the 'app', 'redis', and scale 'worker' to 4 replicas.
# ADDED: --wait flag ensures containers are healthy before the command exits.
docker compose up -d --wait --scale worker=4

echo ""
echo "--- Your evalX environment is now running! ---"
echo ""
docker compose ps

echo ""
echo "--- Showing initial logs for all services for 10 seconds... ---"
# This gives you a quick glance at the startup logs to spot any immediate errors.
docker compose logs --tail="50" --follow &
LOGS_PID=$!
sleep 10
kill $LOGS_PID || true # Kill the log follower, ignoring error if it already exited

echo ""
echo "--- Setup complete. To follow logs continuously, run: ---"
echo "docker compose logs -f"
echo "--- To see logs for a specific service, run (e.g., for a worker): ---"
echo "docker compose logs -f worker"