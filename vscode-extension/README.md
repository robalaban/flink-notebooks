# Flink Notebooks - VSCode Extension

Interactive notebooks for Apache Flink SQL development.

## Features

- **Jupyter-like Interface**: Write and execute Flink SQL in notebook cells
- **Local MiniCluster**: No Docker required - runs Flink via embedded Java process
- **Real-time Streaming**: See streaming query results update in real-time
- **AWS Glue Integration**: Browse and query tables from AWS Glue Data Catalog
- **Rich Output**: Formatted table rendering with automatic pagination
- **Session Management**: Automatic SQL session lifecycle management

## Requirements

- **Node.js** 18+ (for extension development)
- **Java** 11+ (for Flink MiniCluster)
- **Python** 3.11+ (for orchestrator service)
- **AWS Credentials** (optional, for Glue Catalog)

## Installation

### 1. Install Dependencies

```bash
npm install
```

### 2. Start the Orchestrator Service

The orchestrator service must be running before using the extension:

```bash
cd ../orchestrator
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
uvicorn app.main:app --reload
```

### 3. Build the Flink Runtime (first time only)

```bash
cd ../flink-runtime
./gradlew build
```

### 4. Launch Extension

Press `F5` in VSCode to open Extension Development Host.

## Usage

### Creating a Notebook

1. Open Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`)
2. Run: **Flink: New Flink Notebook**
3. A new `.flinknb` file will be created

### Writing SQL

Add a code cell and write Flink SQL:

```sql
SELECT 1 as id, 'Hello Flink!' as message
```

Execute with `Shift+Enter` or click the Run button.

### Managing Cluster

- **Start Cluster**: `Flink: Start Local Cluster`
- **Stop Cluster**: `Flink: Stop Local Cluster`
- **Check Status**: Click the `Flink` status bar item

The cluster starts automatically when you open a notebook (configurable).

### Using Glue Catalog

1. Configure AWS credentials in `~/.aws/credentials` or environment variables
2. Set region in settings: `flink-notebooks.awsRegion`
3. Open **Flink Catalog** view in Explorer
4. Browse databases and tables
5. Right-click table → **Insert Table Reference** to generate SELECT query

## Configuration

Open Settings (`Cmd+,` / `Ctrl+,`) and search for "Flink Notebooks":

| Setting | Default | Description |
|---------|---------|-------------|
| `orchestratorUrl` | `http://localhost:8765` | Orchestrator service URL |
| `javaPath` | (empty) | Path to Java executable |
| `jvmMemory` | `1024m` | JVM heap size for MiniCluster |
| `awsProfile` | (empty) | AWS profile for Glue access |
| `awsRegion` | `us-east-1` | AWS region |
| `autoStartCluster` | `true` | Auto-start cluster on notebook open |

## Keyboard Shortcuts

- `Shift+Enter`: Execute cell
- `Cmd+Enter` / `Ctrl+Enter`: Execute cell and stay
- `Escape`: Cancel cell execution

## Development

### Building

```bash
npm run compile
```

### Watch Mode

```bash
npm run watch
```

### Linting

```bash
npm run lint
```

## Troubleshooting

### Orchestrator Not Available

Ensure the orchestrator service is running:

```bash
cd ../orchestrator
uvicorn app.main:app --reload
```

### Java Not Found

Set `javaPath` in settings or ensure `JAVA_HOME` is set:

```bash
export JAVA_HOME=/path/to/java
```

### Cluster Won't Start

Check orchestrator logs for errors. Common issues:
- Java version < 11
- Port 8083 already in use
- Insufficient memory

## License

Apache License 2.0
