# EvalX - Parallel Code Execution Engine

EvalX is a high-performance code execution engine that runs code in isolated Docker containers with support for multiple programming languages and parallel execution.

## Features

- Multiple language support (Python, Node.js, etc.)
- Parallel code execution
- Configurable concurrent container limits
- REST API endpoints
- Isolated Docker environments

## Setup

1. Install dependencies:
   - Rust (latest stable)
   - Docker
   - curl or Postman for testing

2. Build Docker Images:
   ```bash
   ./build-images.ps1
   ```

3. Build the project:
   ```bash
   cargo build --release
   ```

4. Start the server:
   ```bash
   cargo run --release
   ```

## Docker Image Configuration

The project uses Docker images for each supported language and version. Each Dockerfile is configured to:
- Use slim base images to minimize size
- Set up a working directory
- Execute code passed through environment variables

### Adding New Language Support

To add support for a new language or version:

1. Create a new Dockerfile (e.g., `Dockerfile.ruby3.0`):
   ```dockerfile
   FROM ruby:3.0-slim
   WORKDIR /app
   COPY . .
   CMD ["ruby", "-e", "$CODE"]
   ```

2. Build the image:
   ```bash
   docker build -t ruby:3.0 -f Dockerfile.ruby3.0 .
   ```

3. Use in API requests:
   ```json
   {
     "language": "ruby",
     "version": "3.0",
     "code": "puts 'Hello from Ruby!'",
     "timeout": 10
   }
   ```

## Testing Parallel Execution

The server provides two main endpoints:

1. Single execution: `POST http://localhost:3000/execute`
2. Parallel execution: `POST http://localhost:3000/execute-parallel`

### Example: Testing Parallel Execution with Postman

1. Open Postman and create a new POST request to `http://localhost:3000/execute-parallel`

2. Set the request body to raw JSON with this example:
   ```json
   [
      // Java (21) Test Cases
      {
        "language": "java",
        "version": "java-slim-executor",
        "code": "public class Main { public static void main(String[] args) { System.out.println(\"Hello, World!\"); } }",
        "timeout": 5,
        "memory_limit": "256m"
      },
      {
        "language": "java",
        "version": "java-slim-executor",
        "code": "import java.util.Scanner; public class Main { public static void main(String[] args) { Scanner sc = new Scanner(System.in); String input = sc.nextLine(); System.out.println(\"Echo: \" + input); } }",
        "stdin": "Test Input",
        "timeout": 8,
        "memory_limit": "1g"
      },

      // Python (3.9) Test Cases
      {
        "language": "python",
        "version": "3.9",
        "code": "print(\"Simple Python Test\")",
        "memory_limit": "128m"
      },
      {
        "language": "python",
        "version": "3.9",
        "code": "name = input(\"Enter your name: \")\nprint(f\"Hello, {name}!\")",
        "stdin": "Alice"
      },

      // Java 11 Test Cases
      {
        "language": "java11",
        "version": "11",
        "code": "public class Main { public static void main(String[] args) { System.out.println(\"Java 11 Test\"); } }"
      },
      {
        "language": "java11",
        "version": "11",
        "code": "import java.util.Scanner; public class Main { public static void main(String[] args) { Scanner sc = new Scanner(System.in); int n = sc.nextInt(); System.out.println(n * 2); } }",
        "stdin": "5",
        "timeout": 10
      },

      // C (11) Test Cases
      {
        "language": "c",
        "version": "11",
        "code": "#include <stdio.h>\nint main() { printf(\"C Test\\n\"); return 0; }"
      },
      {
        "language": "c",
        "version": "11",
        "code": "#include <stdio.h>\nint main() { char buffer[100]; fgets(buffer, 100, stdin); printf(\"You entered: %s\", buffer); return 0; }",
        "stdin": "C Input",
        "memory_limit": "512m"
      },

      // C++ (11) Test Cases
      {
        "language": "cpp",
        "version": "11",
        "code": "#include <iostream>\nint main() { std::cout << \"C++ Test\" << std::endl; return 0; }"
      },
      {
        "language": "cpp",
        "version": "11",
        "code": "#include <iostream>\n#include <string>\nint main() { std::string input; std::getline(std::cin, input); std::cout << \"C++ Echo: \" << input << std::endl; return 0; }",
        "stdin": "Hello C++",
        "timeout": 7
      }
  ]
   ```

3. Send the request. You should see the results arrive together, with Task 3 completing first (no sleep), followed by Tasks 1 and 2 (with 2-second delays).

### Example: Testing with curl

```bash
curl -X POST http://localhost:3000/execute-parallel \
  -H "Content-Type: application/json" \
  -d '[
    {
      "language": "python",
      "version": "3.9",
      "code": "print(\"Hello from Python!\")",
      "timeout": 10
    },
    {
      "language": "node",
      "version": "16",
      "code": "console.log(\"Hello from Node!\")",
      "timeout": 10
    }
  ]'
```

## Configuration

Edit `.env` file to configure:
- `MAX_CONCURRENT_CONTAINERS`: Maximum number of containers that can run simultaneously
- `DEFAULT_TIMEOUT`: Default timeout for code execution
- `DOCKER_HOST`: Docker daemon socket location

## Monitoring Parallel Execution

To verify that code is running in parallel:

1. Send requests with different execution times
2. Check that total execution time is approximately equal to the longest individual task
3. Monitor Docker containers:
   ```bash
   docker ps
   ```
   You should see multiple containers running simultaneously

## Performance Testing

To stress test parallel execution:

1. Create a large array of execution requests (10+ items)
2. Include varying execution times
3. Monitor system resources:
   ```bash
   docker stats
   ```
4. Verify that execution time scales with container limit, not request count








{
  "language": "java",
  "version": "java-slim-executor",
  "code": "public class Main { public static void main(String[] args) { System.out.println(\"Task 3dfgxzcv done\"); } }",
  "timeout": 10
}
