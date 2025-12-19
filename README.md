# EvalX
<img width="1046" height="778" alt="image" src="https://github.com/user-attachments/assets/35170b76-223b-4d2b-ac5f-9df934c02614" />

> **High-Performance Parallel Code Execution Engine**

EvalX is a robust, distributed remote code execution (RCE) engine built with **Rust**. It provides a secure, sandboxed environment to compile and execute untrusted code across multiple programming languages simultaneously.

Designed for high throughput and low latency, EvalX leverages **Isolate** for Linux kernel-level sandboxing, **Redis** for distributed task queuing and caching, and **Axum** for a high-performance REST API.

## 🚀 Key Features

  * **Secure Sandboxing:** Uses `isolate` (cgroups/namespaces) to strictly limit memory, CPU, processes, and file access.
  * **Distributed Architecture:** Separates API servers from Execution Workers using a Redis-backed job queue.
  * **Batch Execution:** Supports "Fan-Out/Fan-In" architecture to compile code once and run against hundreds of test cases in parallel.
  * **Smart Caching:** Caches compilation artifacts and execution results to minimize redundant processing.
  * **Observability:** Integrated **Prometheus** metrics and **Grafana** dashboards for real-time monitoring of queue depth, latency, and container usage.
  * **Multi-Language Support:** First-class support for Java, C, C++, Python, Go, Rust, TypeScript, Swift, PHP, Ruby, and R.

-----

## 🛠️ Architecture

EvalX operates on a Producer-Consumer model:

1.  **API Server:** Receives code requests, checks the cache, and pushes tasks to a **Redis Priority Queue**.
2.  **Worker Nodes:** Pull tasks, allocate a pre-warmed sandbox from the `LocalSandboxPool`, execute the code securely, and push results back.
3.  **Notification System:** Uses WebSockets/Long-polling to notify clients immediately upon batch completion.

-----

## ⚡ Quick Start

### Prerequisites

  * **Rust** (Latest Stable)
  * **Docker** & Docker Compose
  * **Isolate** (Must be installed on the host machine)

### 1\. Start Infrastructure

Spin up the supporting services (Redis, Prometheus, Grafana, AlertManager).

```bash
docker compose up -d
```

### 2\. Build the Engine

Or simply Compile the project in release mode for maximum performance.

```bash
./start-dev.sh
```

### 3\. Run the Services

You will need two terminal windows to simulate the distributed environment.

**Terminal 1: Start the API Server**

```bash
./target/release/evalx
# Server listening on http://localhost:3000
```

**Terminal 2: Start the Execution Worker**

```bash
sudo ./target/release/evalx worker
# Worker started, utilizing local sandbox pool
```

-----

## 🔌 API Usage

EvalX uses an asynchronous submission model. You submit a batch of work, receive a token, and poll for the results.

### 1\. Submit Code Execution (Batch)

Send code with multiple test cases (`stdin` inputs).

**Request:**
`POST /execute/batch`

```bash
curl -X POST http://localhost:3000/execute/batch \
-H "Content-Type: application/json" \
-d '[
    {
        "language": "python",
        "version": "3.9",
        "code": "import sys\nname = sys.stdin.read().strip()\nprint(f\"Hello, {name}!\")",
        "stdin": "Alice",
        "timeout": 2
    },
    {
        "stdin": "Bob"
    }
]'
```

**Response:**

```json
{
    "token": "550e8400-e29b-41d4-a716-446655440000",
    "status": "Queued",
    "results": null
}
```

### 2\. Poll for Results

Check the status of your submission using the token returned above.

**Request:**
`GET /submissions/:token`

```bash
curl http://localhost:3000/submissions/550e8400-e29b-41d4-a716-446655440000
```

**Expected Output (Completed):**

```json
{
    "token": "550e8400-e29b-41d4-a716-446655440000",
    "status": "Completed",
    "results": [
        {
            "stdout": "Hello, Alice!\n",
            "stderr": null,
            "exit_code": 0,
            "run_time": 0.045,
            "space_consumed": "6452 KB"
        },
        {
            "stdout": "Hello, Bob!\n",
            "stderr": null,
            "exit_code": 0,
            "run_time": 0.042,
            "space_consumed": "6452 KB"
        }
    ]
}
```

-----

## 🧹 Maintenance

### Clear Cache

If you need to invalidate all cached compilation artifacts and execution results:

```bash
docker exec evalx_redis redis-cli KEYS "evalx:artifact:*" | xargs -r docker exec evalx_redis redis-cli DEL
```

### Emergency Reset

If sandboxes become locked or corrupted due to a crash:

```bash
./emergency_reset.sh
```

-----

## 📊 Supported Languages

| Language | Version | Identifier |
| :--- | :--- | :--- |
| **Java** | OpenJDK 21 | `java21` |
| **C#** | 11 | `csharp` |
| **Kotlin** | 1.9 | `kotlin` |
| **Python** | 3.9 | `python` |
| **Python** | 2.7 | `python` |
| **C** | GCC 11 | `c` |
| **C++** | GCC 11 | `cpp` |
| **Go** | 1.21 | `go` |
| **Javascript** | 18 | `javascript` |
| **TypeScript** | 5.0 | `typescript` |
| **Swift** | 5.9 | `swift` |
| **PHP** | 8.2 | `php` |
| **Ruby** | 3.2 | `ruby` |
| **R** | 4.3 | `r` |
| **SQLite** | 3.43 | `sqlite` |

## Note : Languages can be added simply by defining the configurations in a single file and adding it to the `config/languages`
