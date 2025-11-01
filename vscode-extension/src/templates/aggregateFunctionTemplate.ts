/**
 * Template for creating a Flink Aggregate Function (UDAF)
 */

export function getAggregateFunctionTemplate(
  className: string,
  functionName: string,
  description: string
): string {
  return `import org.apache.flink.table.functions.AggregateFunction;

/**
 * ${description || 'Custom Aggregate Function'}
 *
 * Usage:
 *   CREATE TEMPORARY FUNCTION ${functionName} AS '${className}';
 *   SELECT ${functionName}(column) FROM table GROUP BY key;
 */
public class ${className} extends AggregateFunction<Long, ${className}.Accumulator> {

    /**
     * Accumulator for maintaining state during aggregation.
     */
    public static class Accumulator {
        public long sum = 0L;
        public long count = 0L;

        // Add any fields needed for your aggregation
    }

    /**
     * Create a new accumulator for a new aggregation.
     */
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator();
    }

    /**
     * Get the final aggregated value from the accumulator.
     */
    @Override
    public Long getValue(Accumulator accumulator) {
        if (accumulator.count == 0) {
            return null;
        }
        // TODO: Compute final result from accumulator
        return accumulator.sum / accumulator.count;
    }

    /**
     * Accumulate a new value into the aggregation.
     *
     * @param accumulator The accumulator to update
     * @param value The value to accumulate
     */
    public void accumulate(Accumulator accumulator, Long value) {
        if (value != null) {
            // TODO: Update accumulator with new value
            accumulator.sum += value;
            accumulator.count += 1;
        }
    }

    // Optional: Implement retraction for updating results
    // public void retract(Accumulator accumulator, Long value) {
    //     if (value != null) {
    //         accumulator.sum -= value;
    //         accumulator.count -= 1;
    //     }
    // }

    // Optional: Merge two accumulators (for distributed aggregation)
    // public void merge(Accumulator accumulator, Iterable<Accumulator> others) {
    //     for (Accumulator other : others) {
    //         accumulator.sum += other.sum;
    //         accumulator.count += other.count;
    //     }
    // }

    // Optional: Reset accumulator
    // public void resetAccumulator(Accumulator accumulator) {
    //     accumulator.sum = 0L;
    //     accumulator.count = 0L;
    // }
}
`;
}

export function getAggregateFunctionSqlRegistration(
  functionName: string,
  className: string
): string {
  return `CREATE TEMPORARY FUNCTION ${functionName} AS '${className}'`;
}
