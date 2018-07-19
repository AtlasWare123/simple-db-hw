package simpledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op aggregateOp;
    private List<Tuple> tuples;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (!Op.COUNT.equals(what)) {
            throw new IllegalArgumentException("Op should be COUNT");
        }
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregateOp = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.tuples == null) {
            this.tuples = new ArrayList<>();
        }
        this.tuples.add(tup);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        switch (this.aggregateOp) {
        case COUNT:
            return count(this.tuples, groupByFieldType, groupByField);
        default:
            throw new UnsupportedOperationException(String.format("%s is not supported in IntegerAggregator", this.aggregateOp.toString()));
        }
    }

    private OpIterator count(List<Tuple> tuples, Type groupByFieldType, int groupByField) {
        if (tuples == null || tuples.isEmpty()) {
            throw new NoSuchElementException("No tuple can be processed");
        }

        // for no grouping
        if (groupByField == NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE });
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(tuples.size()));
            return new TupleIterator(td, Collections.singletonList(tuple));
        }

        // for grouping
        Map<Field, Integer> fieldToCountMap = new HashMap<>();
        for (Tuple tuple : tuples) {
            Field key = tuple.getField(groupByField);
            fieldToCountMap.put(key, fieldToCountMap.getOrDefault(key, 0) + 1);
        }
        List<Tuple> result = new ArrayList<>();
        TupleDesc td = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE });
        for (Map.Entry<Field, Integer> entry : fieldToCountMap.entrySet()) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, entry.getKey());
            tuple.setField(1, new IntField(entry.getValue()));
            result.add(tuple);
        }
        return new TupleIterator(td, result);
    }
}
