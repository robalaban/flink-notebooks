package examples;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * Example Scalar Function - Computes hash code of any object.
 *
 * This is a bundled example that demonstrates UDF structure.
 * User-created UDFs in workspace/udfs/ will follow a similar pattern.
 *
 * Usage:
 *   CREATE TEMPORARY FUNCTION hash AS 'examples.HashFunction';
 *   SELECT hash(name), hash(age) FROM users;
 */
public class HashFunction extends ScalarFunction {

    /**
     * Compute hash code for any input value.
     *
     * @param input Any object
     * @return Hash code as integer, or null if input is null
     */
    public Integer eval(Object input) {
        if (input == null) {
            return null;
        }
        return input.hashCode();
    }
}
