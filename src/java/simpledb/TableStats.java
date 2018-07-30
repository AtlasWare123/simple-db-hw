package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    static final int IOCOSTPERPAGE = 1000;
    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    private Object[] histograms;
    private TupleDesc td;
    private int ioCostPerPage;
    private int numPages;
    private int numTuples;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage = ioCostPerPage;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableid);
        this.numPages = ((HeapFile)dbFile).numPages();
        this.numTuples = 0;
        this.td = dbFile.getTupleDesc();
        int[] mins = new int[this.td.numFields()];
        int[] maxs = new int[this.td.numFields()];
        this.histograms = new Object[this.td.numFields()];
        for (int i=0; i<this.td.numFields(); i++) {
            if (this.td.getFieldType(i) == Type.INT_TYPE) {
                mins[i] = Integer.MAX_VALUE;
                maxs[i] = Integer.MIN_VALUE;
            }
        }

        DbFileIterator iter = dbFile.iterator(new TransactionId());
        // calculate min, max value for each column
        try {
            iter.open();
            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                this.numTuples++;
                for (int i=0; i<td.numFields(); i++) {
                    if (this.td.getFieldType(i) == Type.INT_TYPE) {
                        mins[i] = Math.min(mins[i], ((IntField)tuple.getField(i)).getValue());
                        maxs[i] = Math.max(maxs[i], ((IntField)tuple.getField(i)).getValue());
                    }
                }
            }
        } catch (TransactionAbortedException | DbException e) {
            throw new RuntimeException(e);
        }

        // init histograms
        for (int i=0; i<this.td.numFields(); i++) {
            if (this.td.getFieldType(i) == Type.INT_TYPE) {
                this.histograms[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
            } else if (this.td.getFieldType(i) == Type.STRING_TYPE) {
                this.histograms[i] = new StringHistogram(NUM_HIST_BINS);
            } else {
                throw new UnknownError("Unknown type: " + this.td.getFieldType(i));
            }
        }

        // add values to histograms
        try {
            iter.rewind();
            while (iter.hasNext()) {
                Tuple tuple = iter.next();
                for (int i=0; i<this.td.numFields(); i++) {
                    if (this.td.getFieldType(i) == Type.INT_TYPE) {
                        ((IntHistogram)this.histograms[i]).addValue(((IntField)tuple.getField(i)).getValue());
                    } else if (this.td.getFieldType(i) == Type.STRING_TYPE) {
                        ((StringHistogram)this.histograms[i]).addValue(((StringField)tuple.getField(i)).getValue());
                    } else {
                        throw new UnknownError("Unknown type: " + this.td.getFieldType(i));
                    }
                }
            }
        } catch (TransactionAbortedException | DbException e) {
            throw new RuntimeException(e);
        } finally {
            iter.close();
        }
    }

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void setStatsMap(HashMap<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return this.ioCostPerPage * this.numPages;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(selectivityFactor * this.numTuples);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        if (this.td.getFieldType(field) == Type.INT_TYPE) {
            return ((IntHistogram)this.histograms[field]).estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (this.td.getFieldType(field) == Type.STRING_TYPE) {
            return ((StringHistogram)this.histograms[field]).estimateSelectivity(op, ((StringField)constant).getValue());
        } else {
            throw new UnknownError("Unknown type: " + this.td.getFieldType(field));
        }
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
