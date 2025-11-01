# Flink Notebooks

Interactive notebooks for Apache Flink SQL development in Visual Studio Code.

## Features

- **Jupyter-like Interface**: Write and execute Flink SQL in notebook cells
- **Local MiniCluster**: Embedded Flink cluster - no Docker required
- **Real-time Streaming**: See streaming query results update in real-time
- **Catalog Browser**: Browse databases, tables, and schemas
- **Job Monitoring**: Track running Flink jobs with live status updates
- **UDF Support**: Create, build, and auto-register custom User-Defined Functions in Java
- **Rich Output**: Formatted table rendering with automatic pagination

## Requirements

- **Java 17+** - Required for Flink 1.20.0 runtime
  - Java 21+ supported via Gradle toolchain auto-provisioning
- **Gradle** - Bundled via wrapper (no manual installation needed)

## Quick Start

### 1. Install Extension

Install the `.vsix` file via:
- Command Palette: **Extensions: Install from VSIX...**
- Or: `code --install-extension flink-notebooks-0.1.0.vsix`

### 2. Create Your First Notebook

1. Open Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`)
2. Run: **Flink: New Flink Notebook**
3. The extension will automatically start the Flink cluster
4. Add a code cell and write SQL:

```sql
SELECT 1 as id, 'Hello Flink!' as message
```

5. Execute with `Shift+Enter`

## Commands

Access these via Command Palette (`Cmd+Shift+P` / `Ctrl+Shift+P`):

### Cluster Management
- **Flink: Start Local Cluster** - Start embedded Flink MiniCluster
- **Flink: Stop Local Cluster** - Stop the running cluster
- **Flink: Restart Local Cluster** - Restart cluster (useful after building UDFs)

### Notebooks
- **Flink: New Flink Notebook** - Create new `.flinknb` file
- **Flink: Clear All Outputs** - Clear all cell outputs in current notebook

### User-Defined Functions (UDFs)
- **Flink: Create UDF** - Generate new UDF Java file in workspace
- **Flink: Build UDFs** - Compile UDFs to JAR
- **Flink: Register UDFs** - Manually register UDFs in current session

### Monitoring
- **Flink: Open Web UI** - Open Flink Web UI at http://localhost:8081
- **Flink: Refresh Catalog** - Refresh catalog tree view
- **Flink: Refresh Jobs** - Refresh job status

## User Interface

### Status Bar

Click the **Flink** status bar item to see:
- Cluster status (Starting, Running, Stopped, Error)
- Quick actions to start/stop cluster

### Sidebar Views

The Flink sidebar provides:
- **Catalog**: Browse databases, tables, and schemas
  - Right-click table → **Insert Table Reference**
- **Jobs**: Monitor running Flink jobs
  - View job details and metrics
  - Cancel running jobs
  - Open jobs in Web UI

## Configuration

Open Settings (`Cmd+,` / `Ctrl+,`) and search for "Flink Notebooks":

### Cluster Settings
- `flink-notebooks.gatewayPort` - SQL Gateway port (default: 8083)
- `flink-notebooks.javaPath` - Path to Java executable (auto-detected if empty)
- `flink-notebooks.jvmMemory` - JVM heap size (default: 1024m)
- `flink-notebooks.parallelism` - Default parallelism (default: 2)
- `flink-notebooks.taskSlots` - Task slots per TaskManager (default: 2)
- `flink-notebooks.autoStartCluster` - Auto-start on notebook open (default: true)

### Execution Settings
- `flink-notebooks.executionMode` - Execution mode: auto, batch, or streaming (default: auto)
- `flink-notebooks.streamingPollInterval` - Streaming poll interval in ms (default: 500)
- `flink-notebooks.maxStreamingRows` - Max rows in streaming mode (default: 10000)

### UDF Settings
- `flink-notebooks.udfAutoRegister` - Auto-register UDFs on session creation (default: true)
- `flink-notebooks.udfAutoBuild` - Auto-build UDFs when Java files saved (default: true)

### Job Monitoring
- `flink-notebooks.jobRefreshInterval` - Job refresh interval in ms (default: 5000)

## User-Defined Functions (UDFs)

### Creating a UDF

1. Run: **Flink: Create UDF**
2. Choose function type (Scalar, Table, or Aggregate)
3. Enter class name, function name, and description
4. Edit the generated Java file in `workspace/udfs/src/main/java/`
5. UDF automatically builds and registers on save (if auto-build enabled)

### Example UDF

```java
import org.apache.flink.table.functions.ScalarFunction;

public class MyUpperCase extends ScalarFunction {
    public String eval(String input) {
        if (input == null) {
            return null;
        }
        return input.toUpperCase();
    }
}
```

### Using UDFs in SQL

```sql
-- UDFs are auto-registered on session creation
SELECT MyUpperCase(name) FROM users;
```

### Manual Build

```bash
# Build UDFs manually
cd flink-runtime
./gradlew :udfs:shadowJar
```

See `UDF_GUIDE.md` for comprehensive documentation.

## Architecture

```
┌─────────────────────────────┐
│  VSCode Extension           │
│  (TypeScript/Node.js)       │
│  - Notebook UI              │
│  - SQL Gateway REST client  │
│  - Cluster lifecycle mgmt   │
└────────────┬────────────────┘
             │ HTTP (port 8083)
             │
┌────────────▼────────────────┐
│  Flink MiniCluster          │
│  (Java Process)             │
│  - SQL Gateway REST API     │
│  - JobManager + TaskManager │
│  - Web UI (port 8081)       │
└─────────────────────────────┘
```

The extension spawns a Java process running the Flink MiniCluster with SQL Gateway. All SQL queries are executed via REST API at `http://localhost:8083`.

## File Format

Notebooks are saved as `.flinknb` files in JSON format:

```json
{
  "cells": [
    {
      "kind": 2,
      "language": "sql",
      "value": "SELECT * FROM my_table"
    }
  ]
}
```

## Keyboard Shortcuts

- `Shift+Enter` - Execute cell and move to next
- `Cmd/Ctrl+Enter` - Execute cell and stay
- Cell toolbar buttons:
  - **Pause** - Pause streaming results
  - **Resume** - Resume streaming
  - **Cancel** - Stop query execution

## Troubleshooting

### Java Version Error

If you see "Unsupported class file major version":
- Ensure Java 17+ is installed
- Set `flink-notebooks.javaPath` if Java not in PATH
- Gradle will auto-download Java 17 if needed

```bash
# Check Java version
java -version

# macOS install
brew install openjdk@17
```

### Cluster Won't Start

Check these common issues:
- Port 8083 already in use (`lsof -i :8083`)
- Java not found (set `javaPath` in settings)
- Insufficient memory (increase `jvmMemory`)
- Check VSCode Output panel → "Flink Notebooks"

### UDF Build Fails

Common issues:
- Java version mismatch → Use Java 17+
- Missing dependencies → Check `udfs/build.gradle`
- Syntax errors → Check VSCode Problems panel

### Streaming Queries Not Updating

- Ensure table is unbounded (streaming source)
- Check execution mode setting
- Increase `streamingPollInterval` if network is slow

## Advanced Usage

### Custom Connectors

Place connector JARs in `flink-runtime/lib/`:
- Iceberg: `iceberg-flink-runtime-1.20_*.jar`
- Kafka: `flink-sql-connector-kafka-*.jar`
- PostgreSQL CDC: `flink-sql-connector-postgres-cdc-*.jar`

Restart cluster to load new connectors.

### AWS Glue Catalog (Future)

Set AWS configuration:
- `flink-notebooks.awsProfile` - AWS profile name
- `flink-notebooks.awsRegion` - AWS region

Note: Glue catalog integration is planned for future release.

## Resources

- [Apache Flink Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/)
- [Flink SQL Reference](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/sql/overview/)
- [Project Repository](https://github.com/your-username/flink-notebooks)

## License

Apache License 2.0
