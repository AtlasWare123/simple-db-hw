package simpledb;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;  // h
    private int min;
    private int max;
    private double interval;  // w
    private int ntups;

    /**
     * Create a new IntHistogram.
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.interval = (max - min) * 1.0 / buckets;
        this.ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        this.buckets[getIndexByValue(v)]++;
        this.ntups++;
    }

    private int getIndexByValue(int v) {
        return v == max ? this.buckets.length - 1 : (int)((v - this.min) / this.interval);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        switch (op) {
        case EQUALS:
            if (v < this.min || v > this.max) {
                return 0.0;
            }
            return this.buckets[getIndexByValue(v)] * 1.0 / ((int)this.interval + 1) / this.ntups;
        case LESS_THAN:
            if (v <= this.min) {
                return 0.0;
            } else if (v >= this.max) {
                return 1.0;
            }
            int idx = getIndexByValue(v);
            int sum = 0;
            for (int i=0; i<idx; i++) {
                sum += this.buckets[i];
            }
            double left = this.min + idx * this.interval;
            return (sum + (v - left) / this.interval * this.buckets[idx]) / this.ntups;
        case LESS_THAN_OR_EQ:
            return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        case GREATER_THAN:
            return 1.0 - estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
        case GREATER_THAN_OR_EQ:
            return 1.0 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
        case NOT_EQUALS:
            return 1.0 - estimateSelectivity(Predicate.Op.EQUALS, v);
        default:
            return -1.0;
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<this.buckets.length; i++) {
            sb.append(String.format("Bucket %d: %d\n", i, this.buckets[i]));
        }
        return sb.toString();
    }
}
