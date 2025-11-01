/**
 * Template for creating a Flink Table Function (UDTF)
 */

export function getTableFunctionTemplate(
  className: string,
  functionName: string,
  description: string
): string {
  return `import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * ${description || 'Custom Table Function'}
 *
 * Usage:
 *   CREATE TEMPORARY FUNCTION ${functionName} AS '${className}';
 *   SELECT word FROM table_name, LATERAL TABLE(${functionName}(column)) AS T(word);
 */
public class ${className} extends TableFunction<Row> {

    /**
     * Evaluate the function and emit zero or more rows.
     *
     * @param input Input value
     */
    public void eval(String input) {
        if (input == null) {
            return;
        }

        // TODO: Implement your logic here
        // Example: Split string and emit each part as a row
        String[] parts = input.split(",");
        for (String part : parts) {
            collect(Row.of(part.trim()));
        }
    }

    // Optional: Override for initialization logic
    // @Override
    // public void open(FunctionContext context) {
    //     // Initialize resources
    // }

    // Optional: Override for cleanup logic
    // @Override
    // public void close() {
    //     // Clean up resources
    // }
}
`;
}

export function getTableFunctionSqlRegistration(
  functionName: string,
  className: string
): string {
  return `CREATE TEMPORARY FUNCTION ${functionName} AS '${className}'`;
}
