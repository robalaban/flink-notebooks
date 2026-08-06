# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

- use standards
- take your time, there is no rush
- use clean code principles

## Project Overview

Flink Notebooks is a VSCode extension that provides a Jupyter-like notebook experience for authoring and executing Apache Flink SQL queries locally. The project consists of two main components:

1. **Flink Runtime (Java)**: A MiniCluster running Flink SQL Gateway in a local Java process
2. **VSCode Extension (TypeScript)**: The notebook UI that communicates with the runtime via REST API

## Architecture

```
┌─────────────────────────────┐
│  VSCode Extension           │
│  (TypeScript/Node.js)       │
│  - Notebook editor UI       │
│  - Cluster lifecycle mgmt   │
│  - SQL execution            │
│  - Catalog browser          │
└────────────┬────────────────┘
             │ HTTP REST (port 8083)
             │ WebSocket (streaming)
             │
┌────────────▼────────────────┐
│  Flink MiniCluster Runtime  │
│  (Java)                     │
│  - SQL Gateway REST API     │
│  - Flink Web UI (port 8081) │
│  - JobManager + TaskManager │
└─────────────────────────────┘
```

### Key Components

**Java Runtime** (`flink-runtime/`):
- Entry point: `MiniClusterRunner.java` - Starts MiniCluster + SQL Gateway
- Configuration: `conf/flink-conf.yaml` - Flink settings (parallelism, memory, etc.)
- Build: Fat JAR using Gradle Shadow plugin (~9.5GB with all dependencies)
- Dependencies: Flink 1.20.0, Iceberg, AWS SDK, Hadoop, PostgreSQL CDC

**VSCode Extension** (`vscode-extension/`):
- Entry point: `src/extension.ts` - Extension activation
- Services:
  - `clusterManager.ts` - Manages Java process lifecycle
  - `sqlGatewayClient.ts` - HTTP client for SQL Gateway REST API
  - `catalogService.ts` - Queries Flink catalogs (Glue integration)
  - `flinkJobClient.ts` - Monitors running Flink jobs
- Providers:
  - `flinkNotebookController.ts` - Handles cell execution (Shift+Enter)
  - `flinkNotebookSerializer.ts` - Serializes `.flinknb` file format
  - `catalogTreeProvider.ts` - Displays catalog tree view
  - `jobMonitorProvider.ts` - Displays job status tree view

### Communication Flow

1. Extension spawns Java process running `flink-minicluster.jar`
2. Extension waits for SQL Gateway health check at `http://localhost:8083/v1/info`
3. User creates notebook (`.flinknb` file) and types SQL in cells
4. User presses Shift+Enter → Extension sends SQL to Gateway → Results stream back
5. Extension polls for results and renders as markdown tables in cell output

## Build Commands

### Quick Start (Automated)
```bash
./quickstart.sh  # Builds both components, validates prerequisites
```

### Manual Build

**Flink Runtime:**
```bash
cd flink-runtime
./gradlew shadowJar          # Create fat JAR only (faster)
./gradlew build              # Full build with tests
./gradlew run                # Run MiniCluster directly
```

Output: `flink-runtime/build/libs/flink-minicluster.jar`

**VSCode Extension:**
```bash
cd vscode-extension
npm install                  # Install dependencies
npm run compile              # TypeScript → JavaScript
npm run watch                # Watch mode (auto-recompile)
```

Output: `vscode-extension/out/extension.js`

### Development Workflow

1. Build both components (or run `./quickstart.sh`)
2. Open `vscode-extension/` folder in VSCode
3. Press F5 to launch Extension Development Host
4. In new window, create notebook and test features

### Running Tests

Currently manual testing only. See TODO.md for testing needs.

## Configuration

### Flink Configuration (`flink-runtime/conf/flink-conf.yaml`)

Key settings:
- `execution.target: remote` - Gateway connects to MiniCluster at localhost:6123
- `parallelism.default: 2` - SQL queries parallelize across slots
- `taskmanager.numberOfTaskSlots: 2` - Default parallelism
- `state.backend: hashmap` - In-memory state (no persistence)
- `execution.checkpointing.interval: 10s` - Checkpoint frequency

### VSCode Extension Settings

All settings prefixed with `flink-notebooks.*`:
- `gatewayPort` (default: 8083) - SQL Gateway REST API port
- `miniclusterJarPath` - Path to JAR (auto-detected if empty)
- `javaPath` - Java executable path (uses JAVA_HOME or PATH)
- `jvmMemory` (default: 1024m) - JVM heap size
- `parallelism` (default: 2) - Default parallelism level
- `taskSlots` (default: 2) - Task slots per TaskManager
- `awsProfile` - AWS profile for Glue Catalog
- `awsRegion` (default: us-east-1) - AWS region
- `autoStartCluster` (default: true) - Auto-start on notebook open

## Important Patterns

### Session Management
- SQL Gateway requires creating a session before executing queries
- Sessions are managed per notebook by `SessionManager`
- Session ID stored in execution context

### Error Handling
- Axios errors caught and re-thrown with context
- User-friendly error messages via VSCode notifications
- Cluster startup errors include remediation hints

### Result Streaming
- Gateway returns results with `nextResultUri` for pagination
- Extension polls until `resultType === 'EOS'` (end of stream)
- **Known Issue**: Currently only polls once (see TODO.md #3)

### Catalog Integration
- Queries Flink's `information_schema` for metadata
- AWS Glue Catalog supported via `@aws-sdk/client-glue`
- **Known Issue**: Tree insert broken - `findDatabase()` returns null (see TODO.md #1)

## Testing

Currently no automated test suite. Manual testing against real MiniCluster.

Future needs:
- Unit tests for services (ClusterManager, SqlGatewayClient, SessionManager)
- Integration tests with real MiniCluster
- E2E tests for notebook execution
- AWS Glue catalog mocking

## Conventions

### Code Organization
- **Services**: Business logic (e.g., `clusterManager.ts`, `sqlGatewayClient.ts`)
- **Providers**: VSCode integration (e.g., `flinkNotebookController.ts`)
- **Models**: TypeScript interfaces in `models/types.ts`

### Naming
- TypeScript services: `*Service.ts`, `*Client.ts`, `*Manager.ts`
- VSCode providers: `*Provider.ts`, `*Controller.ts`
- Java classes: PascalCase, inner `Config` classes for parameters

### TypeScript
- Full TypeScript usage with custom interfaces
- Avoid `any` type where possible
- Async/Promise based for all I/O

### Git
- Follow conventional commits when possible: `feat:`, `fix:`, `docs:`, etc.

## Key Entry Points

**Java Runtime:**
- `flink-runtime/src/main/java/com/flink/notebooks/MiniClusterRunner.java:main()`
  - Parses CLI args: `--parallelism`, `--taskslots`, `--gateway-port`
  - Starts MiniCluster (JobManager + TaskManager)
  - Starts SQL Gateway REST endpoint

**VSCode Extension:**
- `vscode-extension/src/extension.ts:activate()`
  - Initializes ClusterManager
  - Registers NotebookSerializer, Controller, Providers
  - Registers commands and status bar

## Useful Resources

- Flink Web UI: http://localhost:8081 (when running)
- SQL Gateway API: http://localhost:8083/v1 (when running)
- TODO.md - Comprehensive feature roadmap
- SETUP.md - Detailed setup and troubleshooting
