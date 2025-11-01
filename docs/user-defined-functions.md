# User-Defined Functions (UDFs)

This guide explains how to create and use custom User-Defined Functions (UDFs) in Flink Notebooks.

## Overview

User-Defined Functions (UDFs) extend Flink SQL with custom logic written in Java. Flink Notebooks provides an integrated workflow for creating, building, and using UDFs without leaving the development environment.

## UDF Types

Flink supports four types of UDFs:

1. **Scalar Functions** - Transform one row to one value (e.g., `upper(string)`)
2. **Table Functions** - Transform one row to multiple rows (e.g., `split(string)`)
3. **Aggregate Functions** - Transform multiple rows to one value (e.g., `avg(value)`)
4. **Table Aggregate Functions** - Transform multiple rows to multiple rows

## Quick Start

### Creating a UDF

1. Open Command Palette (Cmd+Shift+P / Ctrl+Shift+P)
2. Run: **Flink: Create UDF**
3. Select UDF type (Scalar, Table, or Aggregate)
4. Enter class name (e.g., `MyUpperCase`)
5. Enter function name for SQL (e.g., `my_upper`)
6. Enter description (optional)

This creates a Java file in `workspace/udfs/src/main/java/` with scaffolding code.

### Building UDFs

After editing your UDF:

1. Save the Java file
2. Run: **Flink: Build UDFs** (or auto-builds if `udfAutoBuild` is enabled)
3. Run: **Flink: Restart Local Cluster** to load the new UDF

### Using UDFs in SQL

UDFs are automatically registered when a session starts (if `udfAutoRegister` is enabled):

```sql
-- Use the UDF directly by class name
SELECT MyUpperCase(name) FROM users;
```

## Minimal Examples

### Scalar Function

A scalar function takes one or more input values and returns a single output value.

**File:** `workspace/udfs/src/main/java/StringReverse.java`

```java
import org.apache.flink.table.functions.ScalarFunction;

public class StringReverse extends ScalarFunction {
    public String eval(String input) {
        if (input == null) {
            return null;
        }
        return new StringBuilder(input).reverse().toString();
    }
}
```

**SQL Usage:**
```sql
SELECT StringReverse('hello') as reversed;
-- Result: olleh

SELECT name, StringReverse(name) as reversed_name
FROM users;
```

### Table Function

A table function takes one row and produces multiple output rows.

**File:** `workspace/udfs/src/main/java/SplitWords.java`

```java
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class SplitWords extends TableFunction<Row> {
    public void eval(String sentence) {
        if (sentence != null) {
            for (String word : sentence.split("\\s+")) {
                collect(Row.of(word, word.length()));
            }
        }
    }
}
```

**SQL Usage:**
```sql
-- Use LATERAL TABLE to expand rows
SELECT word, length
FROM users,
LATERAL TABLE(SplitWords(bio)) AS T(word, length);
```

**Example:**
```sql
-- Input: bio = "Apache Flink is great"
-- Output:
--   word    | length
--   --------+-------
--   Apache  | 6
--   Flink   | 5
--   is      | 2
--   great   | 5
```

### Aggregate Function

An aggregate function combines multiple rows into a single aggregated value.

**File:** `workspace/udfs/src/main/java/WeightedAverage.java`

```java
import org.apache.flink.table.functions.AggregateFunction;

public class WeightedAverage extends AggregateFunction<Double, WeightedAverage.Accumulator> {

    public static class Accumulator {
        public double sum = 0.0;
        public double weightSum = 0.0;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    @Override
    public Double getValue(Accumulator acc) {
        if (acc.weightSum == 0) {
            return null;
        }
        return acc.sum / acc.weightSum;
    }

    public void accumulate(Accumulator acc, Double value, Double weight) {
        if (value != null && weight != null) {
            acc.sum += value * weight;
            acc.weightSum += weight;
        }
    }
}
```

**SQL Usage:**
```sql
SELECT
    product_id,
    WeightedAverage(price, quantity) as avg_weighted_price
FROM orders
GROUP BY product_id;
```

**Example:**
```sql
-- Input:
--   product_id | price | quantity
--   -----------+-------+---------
--   1          | 10.0  | 2
--   1          | 15.0  | 3
--   2          | 20.0  | 1
--   2          | 25.0  | 4

-- Output:
--   product_id | avg_weighted_price
--   -----------+-------------------
--   1          | 13.0  ((10*2 + 15*3) / (2+3))
--   2          | 24.0  ((20*1 + 25*4) / (1+4))
```

## Practical Example: JSON Parsing

A common use case is parsing JSON fields in SQL.

**File:** `workspace/udfs/src/main/java/JsonExtract.java`

```java
import org.apache.flink.table.functions.ScalarFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonExtract extends ScalarFunction {
    private static final ObjectMapper mapper = new ObjectMapper();

    public String eval(String json, String path) {
        if (json == null || path == null) {
            return null;
        }

        try {
            JsonNode node = mapper.readTree(json);
            JsonNode result = node.at(path);
            return result.isMissingNode() ? null : result.asText();
        } catch (Exception e) {
            return null;
        }
    }
}
```

**Dependencies Required:**

Add Jackson to `flink-runtime/udfs/build.gradle`:

```gradle
dependencies {
    compileOnly "org.apache.flink:flink-table-api-java:${flinkVersion}"
    compileOnly "org.apache.flink:flink-table-common:${flinkVersion}"

    // Add Jackson for JSON parsing
    implementation "com.fasterxml.jackson.core:jackson-databind:2.15.2"
}
```

**SQL Usage:**
```sql
SELECT
    id,
    JsonExtract(metadata, '/user/name') as user_name,
    JsonExtract(metadata, '/user/email') as user_email
FROM events
WHERE JsonExtract(metadata, '/event/type') = 'purchase';
```

## UDF Workflow

### Development Cycle

```
1. Create UDF
   ├─> Flink: Create UDF
   └─> Edit Java file in workspace/udfs/src/main/java/

2. Build UDF
   ├─> Auto-build on save (if enabled)
   └─> Or: Flink: Build UDFs

3. Load UDF
   └─> Flink: Restart Local Cluster

4. Test UDF
   └─> Write SQL queries using the UDF
```

### Auto-Build and Auto-Register

By default, UDFs are automatically built and registered:

**Settings:**
- `flink-notebooks.udfAutoBuild`: Auto-build when Java files are saved (default: true)
- `flink-notebooks.udfAutoRegister`: Auto-register when session starts (default: true)

Disable in VSCode settings if you prefer manual control.

### Build System

UDFs are compiled using Gradle:

**Manual Build:**
```bash
cd flink-runtime
./gradlew :udfs:shadowJar
```

**Output:**
- Compiled JAR: `flink-runtime/lib/flink-udfs.jar`
- Contains both bundled examples and workspace UDFs

## Best Practices

### 1. Null Handling

Always handle null inputs gracefully:

```java
public String eval(String input) {
    if (input == null) {
        return null;  // Return null instead of throwing exception
    }
    return input.toUpperCase();
}
```

### 2. Type Safety

Use specific types instead of Object when possible:

```java
// Good
public Double eval(Double x, Double y) {
    return x + y;
}

// Avoid
public Object eval(Object x, Object y) {
    return (Double)x + (Double)y;
}
```

### 3. Performance

Avoid expensive operations in tight loops:

```java
// Bad - Creates regex pattern on every call
public String eval(String input) {
    return input.replaceAll("\\s+", "_");
}

// Good - Reuse compiled pattern
private static final Pattern WHITESPACE = Pattern.compile("\\s+");

public String eval(String input) {
    if (input == null) return null;
    return WHITESPACE.matcher(input).replaceAll("_");
}
```

### 4. Determinism

Mark non-deterministic functions appropriately:

```java
import java.util.UUID;

public class GenerateUUID extends ScalarFunction {
    @Override
    public boolean isDeterministic() {
        return false;  // Different output for same input
    }

    public String eval() {
        return UUID.randomUUID().toString();
    }
}
```

### 5. Resource Management

Use lifecycle methods for expensive resources:

```java
import java.sql.Connection;
import org.apache.flink.table.functions.FunctionContext;

public class DatabaseLookup extends ScalarFunction {
    private Connection conn;

    @Override
    public void open(FunctionContext context) throws Exception {
        // Initialize connection once
        conn = DriverManager.getConnection(jdbcUrl);
    }

    public String eval(Long id) {
        // Use connection for lookups
    }

    @Override
    public void close() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }
}
```

## Advanced Features

### Variable Arguments

UDFs can accept variable numbers of arguments:

```java
public class Concatenate extends ScalarFunction {
    public String eval(String... strings) {
        if (strings == null) return null;
        return String.join(", ", strings);
    }
}
```

**SQL Usage:**
```sql
SELECT Concatenate('a', 'b', 'c');  -- Returns: "a, b, c"
SELECT Concatenate('hello', 'world');  -- Returns: "hello, world"
```

### Method Overloading

Provide multiple signatures for different input types:

```java
public class Add extends ScalarFunction {
    // Add integers
    public Integer eval(Integer a, Integer b) {
        if (a == null || b == null) return null;
        return a + b;
    }

    // Add doubles
    public Double eval(Double a, Double b) {
        if (a == null || b == null) return null;
        return a + b;
    }

    // Add strings (concatenate)
    public String eval(String a, String b) {
        if (a == null || b == null) return null;
        return a + b;
    }
}
```

### Type Inference

For complex return types, implement `getTypeInference()`:

```java
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;

public class ComplexFunction extends ScalarFunction {
    public Row eval(String input) {
        // Implementation
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
            .outputTypeStrategy(callContext -> {
                return Optional.of(DataTypes.ROW(
                    DataTypes.FIELD("field1", DataTypes.STRING()),
                    DataTypes.FIELD("field2", DataTypes.INT())
                ));
            })
            .build();
    }
}
```

## Troubleshooting

### UDF Not Found

**Error:**
```
SQL validation failed: No match found for function signature MyUDF(STRING)
```

**Solutions:**
1. Verify UDF is built: Check for `flink-runtime/lib/flink-udfs.jar`
2. Restart cluster: Run **Flink: Restart Local Cluster**
3. Check auto-register is enabled: Settings > `flink-notebooks.udfAutoRegister`
4. Manually register:
   ```sql
   CREATE TEMPORARY FUNCTION MyUDF AS 'MyUDF';
   ```

### Build Errors

**Error:**
```
error: cannot find symbol
```

**Solutions:**
1. Verify Java syntax is correct
2. Check imports are present
3. Ensure Flink version in `build.gradle` matches (1.20.0)
4. Check VSCode Problems panel for detailed errors

### ClassNotFoundException

**Error:**
```
java.lang.ClassNotFoundException: MyUDF
```

**Solutions:**
1. Rebuild UDFs: **Flink: Build UDFs**
2. Verify class name matches file name
3. Check JAR was created: `ls -lh flink-runtime/lib/flink-udfs.jar`
4. Restart cluster to reload JAR

### Type Mismatch

**Error:**
```
Cannot apply 'MyUDF' to arguments of type '<INTEGER>'
```

**Solutions:**
1. Check eval() method signature matches SQL usage
2. Verify input types in SQL match Java method
3. Add method overload for the required type

## Examples from Bundled UDFs

Flink Notebooks includes example UDFs in `flink-runtime/udfs/src/main/java/examples/`:

### HashFunction

Computes hash code of any object:

```java
package examples;

import org.apache.flink.table.functions.ScalarFunction;

public class HashFunction extends ScalarFunction {
    public Integer eval(Object input) {
        if (input == null) {
            return null;
        }
        return input.hashCode();
    }
}
```

**Usage:**
```sql
SELECT id, examples.HashFunction(email) as email_hash
FROM users;
```

### StringReverseFunction

Reverses a string:

```java
package examples;

import org.apache.flink.table.functions.ScalarFunction;

public class StringReverseFunction extends ScalarFunction {
    public String eval(String input) {
        if (input == null) {
            return null;
        }
        return new StringBuilder(input).reverse().toString();
    }
}
```

**Usage:**
```sql
SELECT examples.StringReverseFunction('hello');  -- Returns: olleh
```

Note: Bundled examples use the `examples` package, so reference them as `examples.FunctionName`.

## References

- Flink UDF Documentation: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/functions/udfs/
- Data Types Mapping: https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/types/
- Project UDF Guide: `UDF_GUIDE.md` in repository root

## Workspace Structure

User UDFs are organized in your workspace:

```
workspace/
└── udfs/
    └── src/
        └── main/
            └── java/
                ├── MyUpperCase.java
                ├── SplitWords.java
                └── WeightedAverage.java
```

This directory is separate from the extension installation and tracked by your project's version control.
