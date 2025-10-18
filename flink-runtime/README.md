# Flink MiniCluster Runtime

Embedded Flink MiniCluster with SQL Gateway for local development.

**Flink Version:** 1.20.0 (Java API only - no Scala dependencies)

## Overview

This module provides a standalone Java application that runs:
- Flink MiniCluster (JobManager + TaskManager in a single JVM)
- SQL Gateway REST API for executing Flink SQL

## Building

Requires Java 11+:

```bash
./gradlew build
```

This creates `build/libs/flink-minicluster.jar` - a fat JAR with all dependencies.

On Windows, use `gradlew.bat` instead of `./gradlew`.

## Running

```bash
java -jar build/libs/flink-minicluster.jar [options]
```

### Options

- `--parallelism <num>` - Set parallelism level (default: 2)
- `--taskslots <num>` - Set task slots per TaskManager (default: 2)
- `--gateway-port <port>` - Set SQL Gateway port (default: 8083)
- `--help` - Show help message

### Example

```bash
# Start with 4 task slots and gateway on port 9000
java -Xmx2g -jar build/libs/flink-minicluster.jar --taskslots 4 --gateway-port 9000
```

## Endpoints

Once running:
- **SQL Gateway**: `http://localhost:8083` (REST API)
- **Flink Web UI**: `http://localhost:8081` (job monitoring)

## Memory Settings

Control JVM heap size with `-Xmx` flag:

```bash
java -Xmx1024m -jar build/libs/flink-minicluster.jar  # 1GB heap
java -Xmx2g -jar build/libs/flink-minicluster.jar     # 2GB heap
```

## Development

The main class is `com.flink.notebooks.MiniClusterRunner`.

To modify:
1. Edit source files in `src/main/java/`
2. Rebuild with `./gradlew build`
3. Test the new JAR

### Gradle Tasks

- `./gradlew build` - Build the project and create fat JAR
- `./gradlew clean` - Clean build artifacts
- `./gradlew run` - Run the application directly
- `./gradlew shadowJar` - Build only the fat JAR

## Notes

- MiniCluster runs entirely in-process (no Docker needed)
- Suitable for local development and testing
- Not for production use
- State is ephemeral (lost on restart)
