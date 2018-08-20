package simpledb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByField;
    private Type groupByFieldType;
    private int aggregateField;
    private Op aggregateOp;
    private List<Tuple> tuples;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.groupByField = gbfield;
        this.groupByFieldType = gbfieldtype;
        this.aggregateField = afield;
        this.aggregateOp = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
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
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        switch (this.aggregateOp) {
        case AVG:
            return avg(this.tuples, this.groupByFieldType, this.groupByField, this.aggregateField);
        case MAX:
            return max(this.tuples, this.groupByFieldType, this.groupByField, this.aggregateField);
        case MIN:
            return min(this.tuples, this.groupByFieldType, this.groupByField, this.aggregateField);
        case SUM:
            return sum(this.tuples, this.groupByFieldType, this.groupByField, this.aggregateField);
        case COUNT:
            return count(this.tuples, this.groupByFieldType, this.groupByField);
        default:
            throw new UnsupportedOperationException(String.format("%s is not supported in IntegerAggregator", this.aggregateOp.toString()));
        }
    }

    private OpIterator min(List<Tuple> tuples, Type groupByFieldType, int groupByField, int aggregateField) {
        if (tuples == null || tuples.isEmpty()) {
            throw new NoSuchElementException("No tuple can be processed");
        }

        // for no grouping
        if (groupByField == NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(tuples.stream().mapToInt(item -> ((IntField)item.getField(aggregateField)).getValue()).min().orElseThrow(NoSuchElementException::new)));
            return new TupleIterator(td, Collections.singletonList(tuple));
        }

        Map<Field, Integer> fieldToMinMap = new HashMap<>();
        for (Tuple tuple : tuples) {
            Field key = tuple.getField(groupByField);
            int value = ((IntField)tuple.getField(aggregateField)).getValue();
            fieldToMinMap.put(key, Math.min(fieldToMinMap.getOrDefault(key, value), value));
        }
        List<Tuple> result = new ArrayList<>();
        TupleDesc td = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE });
        for (Map.Entry<Field, Integer> entry : fieldToMinMap.entrySet()) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, entry.getKey());
            tuple.setField(1, new IntField(entry.getValue()));
            result.add(tuple);
        }
        return new TupleIterator(td, result);
    }

    private OpIterator max(List<Tuple> tuples, Type groupByFieldType, int groupByField, int aggregateField) {
        if (tuples == null || tuples.isEmpty()) {
            throw new NoSuchElementException("No tuple can be processed");
        }

        // for no grouping
        if (groupByField == NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(tuples.stream().mapToInt(item -> ((IntField)item.getField(aggregateField)).getValue()).max().orElseThrow(NoSuchElementException::new)));
            return new TupleIterator(td, Collections.singletonList(tuple));
        }

        Map<Field, Integer> fieldToMaxMap = new HashMap<>();
        for (Tuple tuple : tuples) {
            Field key = tuple.getField(groupByField);
            int value = ((IntField)tuple.getField(aggregateField)).getValue();
            fieldToMaxMap.put(key, Math.max(fieldToMaxMap.getOrDefault(key, value), value));
        }
        List<Tuple> result = new ArrayList<>();
        TupleDesc td = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE });
        for (Map.Entry<Field, Integer> entry : fieldToMaxMap.entrySet()) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, entry.getKey());
            tuple.setField(1, new IntField(entry.getValue()));
            result.add(tuple);
        }
        return new TupleIterator(td, result);
    }

    private OpIterator sum(List<Tuple> tuples, Type groupByFieldType, int groupByField, int aggregateField) {
        if (tuples == null || tuples.isEmpty()) {
            throw new NoSuchElementException("No tuple can be processed");
        }

        // for no grouping
        if (groupByField == NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
            Tuple tuple = new Tuple(td);
            tuple.setField(0, new IntField(tuples.stream().mapToInt(item -> ((IntField)item.getField(aggregateField)).getValue()).sum()));
            return new TupleIterator(td, Collections.singletonList(tuple));
        }

        // for grouping
        Map<Field, Integer> fieldToSumMap = new HashMap<>();
        for (Tuple tuple : tuples) {
            Field key = tuple.getField(groupByField);
            fieldToSumMap.put(key, fieldToSumMap.getOrDefault(key, 0) + ((IntField)tuple.getField(aggregateField)).getValue());
        }
        List<Tuple> result = new ArrayList<>();
        TupleDesc td = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE });
        for (Map.Entry<Field, Integer> entry : fieldToSumMap.entrySet()) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, entry.getKey());
            tuple.setField(1, new IntField(entry.getValue()));
            result.add(tuple);
        }
        return new TupleIterator(td, result);
    }

    private OpIterator avg(List<Tuple> tuples, Type groupByFieldType, int groupByField, int aggregateField) {
        if (tuples == null || tuples.isEmpty()) {
            throw new NoSuchElementException("No tuple can be processed");
        }

        // for no grouping
        if (groupByField == NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
            Tuple tuple = new Tuple(td);
            int sum = tuples.stream().mapToInt(item -> ((IntField)item.getField(aggregateField)).getValue()).sum();
            tuple.setField(0, new IntField(sum / tuples.size()));
            return new TupleIterator(td, Collections.singletonList(tuple));
        }

        // for grouping
        Map<Field, Integer> fieldToSumMap = new HashMap<>();
        Map<Field, Integer> fieldToCountMap = new HashMap<>();
        for (Tuple tuple : tuples) {
            Field key = tuple.getField(groupByField);
            fieldToSumMap.put(key, fieldToSumMap.getOrDefault(key, 0) + ((IntField)tuple.getField(aggregateField)).getValue());
            fieldToCountMap.put(key, fieldToCountMap.getOrDefault(key, 0) + 1);
        }
        List<Tuple> result = new ArrayList<>();
        TupleDesc td = new TupleDesc(new Type[] { groupByFieldType, Type.INT_TYPE });
        for (Field key : fieldToSumMap.keySet()) {
            Tuple tuple = new Tuple(td);
            tuple.setField(0, key);
            tuple.setField(1, new IntField(fieldToSumMap.get(key) / fieldToCountMap.get(key)));
            result.add(tuple);
        }
        return new TupleIterator(td, result);
    }

    private OpIterator count(List<Tuple> tuples, Type groupByFieldType, int groupByField) {
        if (tuples == null || tuples.isEmpty()) {
            throw new NoSuchElementException("No tuple can be processed");
        }

        // for no grouping
        if (groupByField == NO_GROUPING) {
            TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });
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
