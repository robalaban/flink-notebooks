# Flink UDFs - Bundled Examples

This directory contains **bundled example UDFs** that ship with the Flink Notebooks extension.

**Important:** User-created UDFs should be placed in `workspace/udfs/`, not here. This directory is part of the extension installation and should remain clean.

## Bundled Examples

See `src/main/java/examples/`:
- `HashFunction.java` - Computes hash code of any object
- `StringReverseFunction.java` - Reverses a string

These examples demonstrate proper UDF structure for reference.

## Creating Your Own UDFs

### Quick Start

Use the VSCode command: **Flink: Create UDF**

This will:
1. Create a Java file in your **workspace/udfs/** directory (not here)
2. Auto-build the UDF JAR (compiles both workspace and bundled examples)
3. Auto-register the function in your session

### Manual Creation

1. Create a new Java class in `workspace/udfs/src/main/java/`
2. Extend one of Flink's function base classes
3. Implement the required methods
4. Run: **Flink: Build UDFs** (or `./gradlew :udfs:shadowJar`)
5. Run: **Flink: Restart Local Cluster** to load new UDFs

## Example UDF Structure

```java
import org.apache.flink.table.functions.ScalarFunction;

public class MyFunction extends ScalarFunction {
    public String eval(String input) {
        if (input == null) {
            return null;
        }
        // Your logic here
        return input.toUpperCase();
    }
}
```

**SQL Usage:**
```sql
-- Auto-registered on session creation if udfAutoRegister is enabled
SELECT MyFunction(name) FROM users;
```

## Build System

This Gradle subproject:
- Compiles bundled examples from `src/main/java/`
- Compiles workspace UDFs from `workspace/udfs/src/main/java/` (if specified)
- Outputs single JAR to `flink-runtime/lib/flink-udfs.jar`

Both bundled examples and workspace UDFs are included in the same JAR.

## More Resources

For comprehensive UDF documentation, see:
- Project documentation: `UDF_GUIDE.md` at repository root
- [Flink UDF Documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/functions/udfs/)
- [Data Types Mapping](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/types/)
