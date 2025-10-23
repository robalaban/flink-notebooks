# Flink Connectors Guide

The Flink Notebooks runtime uses a **minimal JAR** (123MB) that includes only core Flink and SQL Gateway.

To use additional connectors (Iceberg, Kafka, AWS, etc.), place the connector JARs in the `lib/` directory. They will be automatically loaded at startup.

---

## 📦 Quick Start

1. **Download connector JARs** from Maven Central
2. **Place them** in `flink-runtime/lib/` directory
3. **Restart the cluster** - connectors are loaded automatically
4. **Use in SQL** - connectors are now available to all queries

---

## 🔌 Popular Connectors

### Apache Iceberg (~50MB)
**For:** Iceberg tables, data lakehouse operations

**Download:**
```bash
cd flink-runtime/lib
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.7.1/iceberg-flink-runtime-1.20-1.7.1.jar
```

**Usage:**
```sql
CREATE CATALOG iceberg_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'hadoop',
  'warehouse' = 'file:///tmp/iceberg-warehouse'
);

USE CATALOG iceberg_catalog;
```

---

### AWS SDK Bundle (~150MB)
**For:** S3 access, AWS Glue Catalog, Kinesis

**Download:**
```bash
cd flink-runtime/lib
curl -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.30.16/bundle-2.30.16.jar
```

**Usage:**
```sql
-- S3 filesystem for Iceberg
CREATE CATALOG glue_catalog WITH (
  'type' = 'iceberg',
  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
  'warehouse' = 's3://my-bucket/warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'
);
```

---

### S3 Filesystem (~5MB)
**For:** Reading/writing files from S3

**Download:**
```bash
cd flink-runtime/lib
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-presto/1.20.0/flink-s3-fs-presto-1.20.0.jar
```

**Usage:**
```sql
CREATE TABLE my_table (
  id INT,
  name STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 's3://my-bucket/data/',
  'format' = 'parquet'
);
```

---

### Apache Kafka (~10MB)
**For:** Kafka sources and sinks

**Download:**
```bash
cd flink-runtime/lib
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar
```

**Usage:**
```sql
CREATE TABLE kafka_source (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'user-behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'
);
```

---

### PostgreSQL JDBC + CDC (~5MB total)
**For:** PostgreSQL source/sink, Change Data Capture

**Download:**
```bash
cd flink-runtime/lib
curl -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-postgres-cdc/3.3.0/flink-sql-connector-postgres-cdc-3.3.0.jar
```

**Usage (CDC):**
```sql
CREATE TABLE postgres_cdc (
  id INT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'postgres-cdc',
  'hostname' = 'localhost',
  'port' = '5432',
  'username' = 'postgres',
  'password' = 'postgres',
  'database-name' = 'mydb',
  'schema-name' = 'public',
  'table-name' = 'users'
);
```

---

### MySQL JDBC + CDC
**For:** MySQL source/sink, Change Data Capture

**Download:**
```bash
cd flink-runtime/lib
curl -O https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.1.0/mysql-connector-j-9.1.0.jar
curl -O https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/3.3.0/flink-sql-connector-mysql-cdc-3.3.0.jar
```

---

### Hadoop Dependencies (~54MB)
**For:** Iceberg with Hadoop catalog, HDFS access

⚠️ **Important:** For Iceberg with AWS Glue Catalog, you need Hadoop JARs even though you're using S3. This is because Iceberg's Hadoop catalog implementation requires Hadoop classes.

**Download (All 4 required for Iceberg):**
```bash
cd flink-runtime/lib
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.4.1/hadoop-common-3.4.1.jar
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client/3.4.1/hadoop-client-3.4.1.jar
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar
```

**What you get:**
- `hadoop-common` - Core Hadoop utilities
- `hadoop-client` - POM wrapper
- `hadoop-client-api` - Hadoop client APIs
- `hadoop-client-runtime` - Hadoop runtime dependencies


---

## 📝 Runtime JAR Loading with ADD JAR

You can also load JARs dynamically at runtime using SQL:

```sql
-- Add a JAR at runtime
ADD JAR '/path/to/connector.jar';

-- Add from S3/HDFS
ADD JAR 's3://my-bucket/connectors/iceberg.jar';
ADD JAR 'hdfs:///jars/kafka-connector.jar';

-- List loaded JARs
SHOW JARS;
```

**Note:** JARs added via `ADD JAR` are only available to the current session.

---

## 🔍 How It Works

1. **Startup:** MiniClusterRunner scans `lib/` directory for `*.jar` files
2. **Loading:** JARs are added to the classpath via `DefaultContext.load()`
3. **Availability:** All connectors become available to SQL Gateway sessions
4. **Persistence:** JARs persist across restarts (unlike `ADD JAR`)

---

## 📚 Additional Resources

- [Flink Connectors Documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/overview/)
- [Maven Central Search](https://search.maven.org/search?q=g:org.apache.flink)
- [Flink JAR Statements](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/jar/)
