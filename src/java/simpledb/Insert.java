package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    final private TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});

    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private boolean called;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId) throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        if (this.child == null) {
            throw new DbException("OpIterator is null, can't be opened");
        }
        this.child.open();
    }

    public void close() {
        // some code goes here
        if (this.child != null) {
            this.child.close();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (this.child == null) {
            throw new DbException("OpIterator is null, can't rewind it");
        }
        this.child.rewind();
        this.called = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.called) {
            return null;
        }
        this.called = true;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(this.tableId);
        int cnt = 0;
        while (child.hasNext()) {
            try {
                dbFile.insertTuple(this.tid, this.child.next());
                cnt++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple result = new Tuple(this.td);
        result.setField(0, new IntField(cnt));
        return result;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if (children != null && children.length > 0) {
            this.child = children[0];
        }
    }
}
