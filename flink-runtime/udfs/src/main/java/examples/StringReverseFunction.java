package examples;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Example Scalar Function - Reverses a string.
 *
 * This is a bundled example that demonstrates UDF structure.
 * User-created UDFs in workspace/udfs/ will follow a similar pattern.
 *
 * Usage:
 *   CREATE TEMPORARY FUNCTION str_reverse AS 'examples.StringReverseFunction';
 *   SELECT str_reverse(name) FROM users;
 */
public class StringReverseFunction extends ScalarFunction {

    /**
     * Reverse a string.
     *
     * @param input Input string
     * @return Reversed string, or null if input is null
     */
    public String eval(String input) {
        if (input == null) {
            return null;
        }
        return new StringBuilder(input).reverse().toString();
    }
}
