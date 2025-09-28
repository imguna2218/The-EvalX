#!/bin/bash

# Bash script to build all Docker images

echo "Building Python 3.9 Image..."
docker build -t python:3.9 -f Docker/python/Dockerfile.python3.9 .
if [ $? -ne 0 ]; then exit 1; fi

echo "Building Java 21 Image..."
docker build -t java-slim-executor -f Docker/java/Dockerfile.java21 .
if [ $? -ne 0 ]; then exit 1; fi

echo "Building Java 11 Image..."
docker build -t java11-slim-executor -f Docker/java/Dockerfile.java11 .
if [ $? -ne 0 ]; then exit 1; fi

echo "Building C (GCC 11) Image..."
docker build -f Docker/c/Dockerfile-c11 -t gcc-slim-executor .
if [ $? -ne 0 ]; then exit 1; fi

echo "Building C++ (GCC 11) Image..."
docker build -f Docker/cpp/Dockerfile-cpp11 -t gcc:11 .
if [ $? -ne 0 ]; then exit 1; fi

# --- ADDED: Build step for Redis ---
echo "Building Redis Image..."
docker build -t evalx-redis -f Docker/redis/Dockerfile.redis .
if [ $? -ne 0 ]; then exit 1; fi

echo "All Docker images built successfully!"