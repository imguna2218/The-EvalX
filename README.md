# EvalX: High-Throughput Distributed Code Execution Engine

This repository contains the local deployment instructions and API documentation for EvalX. Due to the strict system-level configurations and custom Linux-native chroot isolation required for the execution workers, the entire distributed engine is packaged into a standalone Docker environment for seamless local testing.

Manually downloading and configuring the isolated chroot environments and distributed dependencies requires a complex, multi-step system setup. I eliminated that friction entirely—just pull our pre-configured Docker image and follow the steps below to initialize the cluster and start executing payloads immediately.

## Prerequisites

* Docker engine installed and running.
* A configured `.env` file in the root directory.

## 1. Environment Configuration

Create a `.env` file in the same directory where you run the Docker commands and populate it with the required AWS SQS credentials.

```env
# .env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=your_aws_region
SQS_QUEUE_URL=your_sqs_queue_url

```

## 2. Initialize the Cluster Architecture

The setup requires an isolated Docker network, a Redis cache, the API gateway, and the execution worker.

**Create the Network & Cleanup Old Containers:**

```bash
docker network create evalx-net || true
docker rm -f evalx-api evalx-worker evalx-redis || true

```

**Pull the Engine Image:**

```bash
docker pull imguna2218/the-evalx:latest

```

**Start the Redis Cache:**

```bash
docker run -d --name evalx-redis --network evalx-net redis:alpine

```

**Start the API Server:**

```bash
docker run -d \
  --name evalx-api \
  --network evalx-net \
  -e REDIS_HOST=evalx-redis \
  --env-file .env \
  -p 3000:3000 \
  imguna2218/the-evalx:latest /app/evalx

```

**Start the Execution Worker:**

```bash
docker run -d \
  --name evalx-worker \
  --network evalx-net \
  -e REDIS_HOST=evalx-redis \
  --env-file .env \
  imguna2218/the-evalx:latest /app/evalx worker

```

## 3. Verify Deployment

Check the container logs to ensure the system is securely connected to Redis and SQS.

```bash
docker logs evalx-api
docker logs evalx-worker

```

## 4. Submit Code for Execution

The API accepts raw code payloads and returns a unique submission token for asynchronous polling.

**Batch Test Case Execution**

```bash
curl -X POST http://localhost:3000/execute/batch \
-H "Content-Type: application/json" \
-d '[
  {
    "language": "java21",
    "version": "21",
    "code": "import java.util.Scanner;\npublic class Main {\n    public static void main(String[] args) {\n        Scanner scanner = new Scanner(System.in);\n        String name = scanner.nextLine();\n        System.out.println(\"Hello, \" + name);\n    }\n}",
    "stdin": "Batch1"
  },
  {
    "stdin": "Batch2"
  }
]'

```

**Single Test Case Execution**

```bash
curl -X POST http://localhost:3000/execute \
-H "Content-Type: application/json" \
-d '{
  "language": "python",
  "version": "3.9",
  "code": "name = input()\nprint(f\"Hello, {name}\")",
  "stdin": "World"
}'

```

## 5. Retrieve Execution Results

Use the token returned from the `POST` request to fetch the standard output, execution time, and memory metrics.

```bash
curl http://localhost:3000/submissions/{token}

```
