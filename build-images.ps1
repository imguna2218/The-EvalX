# PowerShell script to build all Docker images

Write-Host "Building Python 3.9 Image..."
docker build -t python:3.9 -f Docker/python/Dockerfile.python3.9 .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Building Java 21 Image..."
docker build -t java-slim-executor -f Docker/java/Dockerfile.java21 .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Building Java 11 Image..."
docker build -t java11-slim-executor -f Docker/java/Dockerfile.java11 .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Building C (GCC 11) Image..."
docker build -f Docker/c/Dockerfile-c11 -t gcc-slim-executor .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Building C++ (GCC 11) Image..."
docker build -f Docker/cpp/Dockerfile-cpp11 -t gcc:11 .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "All Docker images built successfully!"
