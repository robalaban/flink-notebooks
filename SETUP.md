# Flink Notebooks - Complete Setup Guide

This guide walks you through setting up Flink Notebooks from scratch.

## Prerequisites

### Required Software

1. **Java 11 or later**
   ```bash
   java -version  # Should show 11+
   ```

2. **Node.js 18 or later**
   ```bash
   node --version  # Should show 18+
   ```

Note: Gradle is included via the wrapper script - no separate installation needed.

### Optional (for AWS Glue)

- AWS Account with Glue access
- AWS CLI configured: `aws configure`

## Step-by-Step Setup

### 1. Build the Flink Runtime

The Flink runtime is a Java application that runs the MiniCluster and SQL Gateway.

```bash
cd flink-runtime
./gradlew build
```

This creates `build/libs/flink-minicluster.jar` (~9500MB with dependencies).

On Windows, use `gradlew.bat build` instead.

**Verification:**
```bash
java -jar build/libs/flink-minicluster.jar --help
```

You should see the help message.

### 2. Build the VSCode Extension

```bash
cd ../vscode-extension && npm install && npm run compile
```

### 3. Launch the Extension

1. Open the `vscode-extension/` folder in VSCode
2. Naavigate to `out/extensions.js`
3. Press `F5` to launch the Extension Development Host
4. A new VSCode window will open with the extension loaded

### 4. Create Your First Notebook

In the Extension Development Host window:

1. Open Command Palette: `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
2. Run: **Flink: Start Local Cluster**
3. Status bar shows `Flink: Running`
4. Type: **Flink: New Flink Notebook**
5. A new notebook file opens

### 6. Execute Your First Query

In the notebook cell, type:

```sql
SELECT 1 as id, 'Hello Flink!' as message
```

Press `Shift+Enter` to execute. You should see the result appear below the cell.

## Troubleshooting

### "Failed to start cluster"

**Problem:** MiniCluster won't start.

**Solutions:**

1. **Java not found**
   ```bash
   # Set JAVA_HOME
   export JAVA_HOME=/path/to/java
   ```

   Or configure in VSCode settings: `flink-notebooks.javaPath`

2. **Port 8083 in use**
   ```bash
   # Check what's using port 8083
   lsof -i :8083  # Mac/Linux
   netstat -ano | findstr :8083  # Windows
   ```

3. **Not enough memory**

   Lower memory in settings: `flink-notebooks.jvmMemory` = `512m`

4. **JAR not found**

   Verify JAR exists: `ls flink-runtime/build/libs/flink-minicluster.jar`

   If not, rebuild: `cd flink-runtime && ./gradlew build`

### "Session creation failed"

**Problem:** Can't create SQL session.

**Solution:**
1. Ensure cluster is running (status bar shows "Running")
2. Check gateway is accessible: `curl http://localhost:8083/v1/info`
3. Restart cluster: **Flink: Stop Cluster** → **Flink: Start Cluster**

### "AWS Glue catalog not loading"

**Problem:** Catalog tree is empty or shows error.

**Solutions:**

1. **No AWS credentials**
   ```bash
   aws configure
   ```

2. **Wrong region**

   Update setting: `flink-notebooks.awsRegion`

3. **No Glue databases**

   Verify in AWS Console that you have databases in Glue

## Configuration

### Extension Settings

Open Settings (`Cmd+,`) and search for "Flink":

| Setting | Recommended | Description |
|---------|-------------|-------------|
| `gatewayPort` | `8083` | SQL Gateway REST API port |
| `jvmMemory` | `1024m` | Increase for larger datasets (`2g`, `4g`) |
| `autoStartCluster` | `true` | Auto-start on notebook open |
| `awsProfile` | (your profile) | AWS profile to use |
| `awsRegion` | (your region) | AWS region for Glue |
| `parallelism` | `2` | Default parallelism level |
| `taskSlots` | `2` | Task slots per TaskManager |


## Getting Help

- Check [README.md](README.md) for feature overview
- Open an issue on GitHub for bugs

