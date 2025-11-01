import org.apache.flink.table.functions.ScalarFunction;

/**
 * Generates a random UUID
 *
 * Usage:
 *   CREATE TEMPORARY FUNCTION random_uuid AS 'RandomUUID';
 *   SELECT random_uuid(column) FROM table;
 */
public class RandomUUID extends ScalarFunction {

    /**
     * Evaluate the function.
     *
     * @param input Input value
     * @return Computed result, or null if input is null
     */
    public String eval() {
        java.util.UUID uuid = java.util.UUID.randomUUID();
        return uuid.toString();
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
    @Override
    public boolean isDeterministic() {
        return false; // For random, current time, etc.
    }
}
