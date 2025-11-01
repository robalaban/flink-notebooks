/**
 * Template for creating a Flink Scalar Function (UDF)
 */

export function getScalarFunctionTemplate(
  className: string,
  functionName: string,
  description: string
): string {
  return `import org.apache.flink.table.functions.ScalarFunction;

/**
 * ${description || 'Custom Scalar Function'}
 *
 * Usage:
 *   CREATE TEMPORARY FUNCTION ${functionName} AS '${className}';
 *   SELECT ${functionName}(column) FROM table;
 */
public class ${className} extends ScalarFunction {

    /**
     * Evaluate the function.
     *
     * @param input Input value
     * @return Computed result, or null if input is null
     */
    public String eval(String input) {
        if (input == null) {
            return null;
        }

        // TODO: Implement your logic here
        return input;
    }

    // Optional: Override for initialization logic
    // @Override
    // public void open(FunctionContext context) {
    //     // Initialize resources (e.g., database connections, caches)
    // }

    // Optional: Override for cleanup logic
    // @Override
    // public void close() {
    //     // Clean up resources
    // }

    // Optional: Mark as non-deterministic if function returns different values for same input
    // @Override
    // public boolean isDeterministic() {
    //     return false; // For random, current time, etc.
    // }
}
`;
}

export function getScalarFunctionSqlRegistration(
  functionName: string,
  className: string
): string {
  return `CREATE TEMPORARY FUNCTION ${functionName} AS '${className}'`;
}
