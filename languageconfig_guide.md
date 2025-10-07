# ⚙️ EvalX Language Configuration Guide

> Welcome to the configuration hub for the `evalX` execution engine\! This guide provides everything you need to know to add, modify, or understand how languages are defined in the system. The engine is designed to be completely **configuration-driven**, meaning you can add new languages without ever touching the core Rust source code.

-----

## 📁 File Structure

All language definitions reside within this `config/languages/` directory. To ensure proper discovery at startup, you must follow a specific nested structure.

The system automatically scans for any `.toml` file matching the pattern: `config/languages/<language_name>_language/v<version>.toml`.

```
config/
└── languages/
    ├── README.md               # This guide
    ├── c_language/
    │   └── v11.toml
    ├── java_language/
    │   ├── v11.toml
    │   └── v21.toml
    └── python_language/
        └── v3.9.toml
```

  * `<language_name>`: The common, lowercase name of the language (e.g., `python`, `go`). This is used to form the API name.
  * `<version>`: The specific version you are defining, prefixed with a literal `v` (e.g., `v3.9`, `v1.21`).

**Example**: To add support for Go version 1.21, you would create the file: `config/languages/go_language/v1.21.toml`.

-----

## 📜 TOML Schema Definition

Each language version `.toml` file acts as an instruction manual for the execution engine. It must contain all the necessary details for compiling and running code for that specific language version.

### **Top-Level Fields**

These fields define the fundamental properties of the language.

  * `is_compiled` (Boolean, **Required**):

      * Defines if the language needs a separate compilation step.
      * `true` for languages like C, C++, Java, Go.
      * `false` for interpreted languages like Python, JavaScript.

  * `source_filename` (String, **Required**):

      * The name given to the user's code when it's saved inside the sandbox.
      * Example: `"main.c"`, `"Main.java"`, `"main.py"`.

  * `executable_filename` (String, **Required**):

      * For compiled languages, this is the name of the binary produced by the compiler (e.g., `"main"`).
      * For interpreted languages, this should be the same as `source_filename` (e.g., `"main.py"`).

  * `chroot_path` (String, *Optional*):

      * Specifies a path on the host machine to a "clean room" directory. If present, the sandbox will mount this path as its root (`/`). This is critical for runtimes like the JVM that need a self-contained filesystem.

  * `env_vars` (Table, *Optional*):

      * A key-value map of environment variables to be set inside the sandbox.
      * Example: `env_vars = { NODE_PATH = "/usr/lib/nodejs" }`.

### **`[compile]` Section**

This table defines how to compile the source code.

  * `command` (Array of Strings, **Required**):

      * The full command and its arguments for the compiler.
      * For interpreted languages, use a harmless command like `["echo", "no-compile"]`.

  * `limits` (Table, *Optional*):

      * Specific resource limits for the **compilation step only**. If this is omitted, the default `[limits]` section will be used. This is useful for memory-hungry compilers like `javac`.

### **`[run]` Section**

This table defines how to execute the compiled artifact or script.

  * `command` (Array of Strings, **Required**):

      * The full command and its arguments for execution.
      * Example: `["./main"]` for a C binary or `["/usr/bin/python3", "main.py"]` for a Python script.

  * `limits` (Table, *Optional*):

      * Specific resource limits for the **run step only**. If omitted, the default `[limits]` section will be used.

### **`[limits]` Section**

This table defines the default resource limits for the sandbox. These values are used if not overridden in the `[compile]` or `[run]` sections.

  * `time_s` (Integer, **Required**): The maximum CPU time the code can use, in seconds. Must be between 1 and 60.
  * `memory_kb` (Integer, **Required**): The maximum memory the code can use, in kilobytes.
  * `processes` (Integer, **Required**): The maximum number of processes the code can spawn inside the sandbox. This is a crucial security limit to prevent fork bombs.

-----

## ✨ Complete Examples

Here are two complete, commented examples demonstrating how to define a compiled and an interpreted language.

### Example 1: Compiled Language (`cpp_language/v11.toml`)

```toml
# Defines a compiled language (C++11)
is_compiled = true
source_filename = "main.cpp"
executable_filename = "main" # The output binary from the compiler

# No chroot_path is needed; it will use the standard system directories for gcc.

[compile]
# The command to run to compile the code.
command = ["/usr/bin/g++", "-o", "main", "main.cpp"]
# No specific compile limits are set, so it will use the default [limits] below.

[run]
# The command to run the compiled binary.
command = ["./main"]
# No specific run limits are set, so it will use the default [limits] below.

[limits]
# Default limits for both compile and run steps.
time_s = 10
memory_kb = 262144 # 256MB
processes = 10
```

### Example 2: Interpreted Language (`python_language/v3.9.toml`)

```toml
# Defines an interpreted language (Python 3.9)
is_compiled = false
source_filename = "main.py"
executable_filename = "main.py" # For interpreted languages, this matches the source.

# Environment variables needed for Python's module system inside the sandbox.
env_vars = { PYTHONPATH = "/usr/lib/python3.10" }

[compile]
# Interpreted languages don't have a real compile step, so we use a placeholder.
command = ["echo", "no-compile"]

[run]
# The command to execute the script with the python interpreter.
command = ["/usr/bin/python3", "main.py"]

[limits]
# Default resource limits. Interpreted languages often need less memory than compilers.
time_s = 5
memory_kb = 131072 # 128MB
processes = 5
```