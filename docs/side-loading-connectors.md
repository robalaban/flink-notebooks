# Side-Loading Connector JARs

This guide explains how to add external connector JARs (Kafka, Iceberg, PostgreSQL, etc.) to Flink Notebooks.

## Overview

Flink connectors are packaged as JAR files that extend Flink's capabilities to read/write from various data sources. By default, Flink Notebooks includes only core Flink functionality. To use connectors like Kafka, Iceberg, or CDC connectors, you must add their JAR files to the runtime classpath.

## Installation Location

Place connector JAR files in the `flink-runtime/lib/` directory:

```
flink-runtime/
├── build/libs/
│   └── flink-minicluster.jar
├── lib/
│   ├── flink-udfs.jar           (auto-generated)
│   ├── kafka-connector.jar      (you add this)
│   ├── iceberg-connector.jar    (you add this)
│   └── postgres-cdc.jar         (you add this)
└── conf/
    └── flink-conf.yaml
```

## Installation Steps

### 1. Download Connector JAR

Download the appropriate connector JAR for Flink 1.20.x from Maven Central or the Apache Flink website.

**Important:** Ensure the connector version matches Flink 1.20.x. Mismatched versions will cause runtime errors.

### 2. Copy to lib/ Directory

```bash
cp /path/to/connector.jar flink-runtime/lib/
```

### 3. Restart Cluster

The cluster must be restarted to load new JARs:

1. Open Command Palette (Cmd+Shift+P / Ctrl+Shift+P)
2. Run: **Flink: Restart Local Cluster**

Or stop and start manually:
- **Flink: Stop Local Cluster**
- **Flink: Start Local Cluster**

### 4. Verify Installation

Check the Flink Notebooks output panel (View > Output > "Flink Notebooks") for log messages confirming JAR loading:

```
Found 3 connector JAR(s) in /path/to/lib
  - flink-connector-kafka-3.3.0-1.20.jar
  - iceberg-flink-runtime-1.20_1.7.1.jar
  - flink-sql-connector-postgres-cdc-3.3.0.jar
```

## Examples

### Apache Kafka Connector

**Use Case:** Read/write streaming data from Kafka topics.

**Download:**
```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
  -P flink-runtime/lib/
```

**SQL Example:**
```sql
-- Create Kafka source table
CREATE TABLE orders (
  order_id BIGINT,
  product STRING,
  amount DECIMAL(10, 2),
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'flink-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- Query streaming data
SELECT product, SUM(amount) as total
FROM orders
GROUP BY product;
```

### Apache Iceberg Connector

**Use Case:** Read/write data lake tables with ACID guarantees.

**Download:**
```bash
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.1/iceberg-flink-runtime-1.20-1.7.1.jar \
  -P flink-runtime/lib/
```

**Additional Requirements:**

Iceberg requires Hadoop dependencies for file system access:

```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
  -P flink-runtime/lib/
```

**SQL Example:**
```sql
-- Create Iceberg catalog
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 'file:///tmp/iceberg-warehouse'
);

USE CATALOG iceberg_catalog;

-- Create database and table
CREATE DATABASE IF NOT EXISTS my_db;
USE my_db;

CREATE TABLE users (
  id BIGINT,
  name STRING,
  email STRING,
  created_at TIMESTAMP(3)
) WITH (
  'format-version' = '2'
);

-- Insert data
INSERT INTO users VALUES
  (1, 'Alice', 'alice@example.com', CURRENT_TIMESTAMP),
  (2, 'Bob', 'bob@example.com', CURRENT_TIMESTAMP);

-- Query data
SELECT * FROM users;
```

### PostgreSQL CDC Connector

**Use Case:** Stream change data capture (CDC) from PostgreSQL databases.

**Download:**
```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.3.0/flink-sql-connector-postgres-cdc-3.3.0.jar \
  -P flink-runtime/lib/
```

**Prerequisites:**

PostgreSQL must have logical replication enabled:

```sql
-- In PostgreSQL, set in postgresql.conf:
wal_level = logical
max_wal_senders = 4
max_replication_slots = 4
```

**SQL Example:**
```sql
-- Create CDC source table
CREATE TABLE users_cdc (
  id BIGINT,
  name STRING,
  email STRING,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'mydb',
  'schema-name' = 'public',
  'table-name' = 'users',
  'decoding.plugin.name' = 'pgoutput'
);

-- Stream changes in real-time
SELECT * FROM users_cdc;
```

## AWS S3 File System Support

To read/write files from S3, add the S3 file system connector:

```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/1.20.0/flink-s3-fs-hadoop-1.20.0.jar \
  -P flink-runtime/lib/
```

**SQL Example:**
```sql
CREATE TABLE s3_data (
  id BIGINT,
  value STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://my-bucket/data/',
  'format' = 'json'
);

SELECT * FROM s3_data;
```

**AWS Credentials:**

Set credentials via environment variables before starting the cluster:

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

Then restart the cluster to pick up the environment variables.

## Troubleshooting

### ClassNotFoundException

**Error:**
```
Caused by: java.lang.ClassNotFoundException: org.apache.kafka.clients.consumer.ConsumerRecord
```

**Cause:** Connector JAR not in classpath.

**Solution:**
1. Verify JAR is in `flink-runtime/lib/`
2. Restart cluster
3. Check Output panel for JAR loading confirmation

### Version Mismatch

**Error:**
```
org.apache.flink.table.api.ValidationException: Could not find required connector dependencies
```

**Cause:** Connector version incompatible with Flink 1.20.x.

**Solution:** Download the correct connector version:
- Kafka: `flink-sql-connector-kafka-3.3.0-1.20.jar`
- Iceberg: `iceberg-flink-runtime-1.20-*.jar`
- Check connector documentation for Flink 1.20 compatibility

### JAR Conflicts

**Error:**
```
java.lang.LinkageError: loader constraint violation
```

**Cause:** Multiple JAR files contain conflicting versions of the same class.

**Solution:**
1. Remove duplicate or conflicting JARs from `lib/`
2. Use "uber" or "fat" JAR variants that bundle dependencies
3. Restart cluster

### Missing Hadoop Dependencies

**Error (Iceberg):**
```
java.lang.NoClassDefFoundError: org/apache/hadoop/conf/Configuration
```

**Cause:** Iceberg requires Hadoop libraries for file system access.

**Solution:** Add Hadoop uber JAR:
```bash
wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
  -P flink-runtime/lib/
```

## Finding Connector JARs

### Maven Central

Search for Flink connectors at: https://search.maven.org/

Filter by:
- Group ID: `org.apache.flink` or `org.apache.iceberg`
- Artifact ID: `flink-sql-connector-*` or `flink-connector-*`
- Version: Compatible with Flink 1.20.x

### Official Flink Downloads

Visit: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/overview/

Each connector page includes download links and version compatibility matrices.

## Best Practices

1. **Version Compatibility:** Always use connectors built for Flink 1.20.x. Check connector documentation.

2. **Fat JARs:** Prefer "uber" or "fat" JAR variants that bundle dependencies to avoid classpath conflicts.

3. **Testing:** Test new connectors in a separate notebook before using in production queries.

4. **Documentation:** Keep a list of installed connectors and their versions in a project README.

5. **Cleanup:** Remove unused connector JARs to reduce cluster startup time and memory usage.

6. **Restart Required:** Always restart the cluster after adding/removing JARs. Changes are not applied until restart.

## Advanced: Custom Connector Path

By default, Flink Notebooks loads JARs from `flink-runtime/lib/`. To use a custom directory:

1. Open VSCode Settings (Cmd+, or Ctrl+,)
2. Search for "Flink Notebooks"
3. Set **Connector Library Path** to your custom directory
4. Restart the cluster

This is useful for sharing connectors across multiple projects.

## References

- Flink Connectors Overview: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/overview/
- Maven Central Repository: https://search.maven.org/
- Iceberg Documentation: https://iceberg.apache.org/docs/latest/flink/
- Kafka Connector Guide: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/kafka/
