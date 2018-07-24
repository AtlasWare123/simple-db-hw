package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TupleDesc td = new TupleDesc(new Type[] { Type.INT_TYPE });

    private TransactionId tid;
    private OpIterator child;
    private boolean called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
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
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.called) {
            return null;
        }
        this.called = true;
        int cnt = 0;
        while (this.child.hasNext()) {
            try {
                Database.getBufferPool().deleteTuple(this.tid, this.child.next());
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
