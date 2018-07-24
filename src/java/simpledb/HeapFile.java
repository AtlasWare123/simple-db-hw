package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return this.file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        if (pid.getPageNumber() < 0 || pid.getPageNumber() >= this.numPages()) {
            throw new NoSuchElementException(String.format("Page number: %d doesn't exist", pid.getPageNumber()));
        }
        RandomAccessFile fin = null;
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            fin = new RandomAccessFile(this.file, "r");
            fin.skipBytes(pid.getPageNumber() * BufferPool.getPageSize());
            fin.read(data, 0, data.length);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fin != null) {
                try {
                    fin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        try {
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile fout = null;
        try {
            fout = new RandomAccessFile(this.file, "rw");
            fout.skipBytes(page.getId().getPageNumber() * BufferPool.getPageSize());
            fout.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)((this.file.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        for (int i = 0; i <= this.numPages(); i++) {
            HeapPageId hpId = new HeapPageId(getId(), i);  // HeapFile id is associated with table id
            HeapPage page = i < this.numPages() ?
                    (HeapPage) Database.getBufferPool().getPage(tid, hpId, Permissions.READ_WRITE) :
                    new HeapPage(new HeapPageId(this.getId(), i), HeapPage.createEmptyPageData());
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                this.writePage(page);
                return new ArrayList<>(Collections.singletonList(page));
            }
        }
        throw new DbException("Failed to insert this tuple");
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != this.getId() || pid.getPageNumber() >= this.numPages()) {
            throw new DbException("The tuple doesn't exist in this table");
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(Collections.singleton(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int curPage = -1;
            private Iterator<Tuple> iter = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.curPage = 0;
                while (this.curPage < numPages()) {
                    if (this.iter == null) {
                        this.iter = ((HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(getId(), this.curPage), null)).iterator();
                    }
                    if (!this.iter.hasNext()) {
                        this.curPage++;
                        this.iter = null;
                    } else {
                        return;
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return this.curPage < numPages() && this.iter != null && this.iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (this.curPage == -1) {
                    throw new NoSuchElementException("iterator has closed");
                }
                Tuple ret = null;
                if (this.iter.hasNext()) {
                    ret = this.iter.next();
                }
                if (!this.iter.hasNext()) {
                    this.curPage++;
                    while (this.curPage < numPages()) {
                        this.iter = ((HeapPage) Database.getBufferPool().getPage(null, new HeapPageId(getId(), this.curPage), null)).iterator();
                        if (!this.iter.hasNext()) {
                            this.curPage++;
                        } else {
                            break;
                        }
                    }
                }
                if (ret == null) {
                    throw new NoSuchElementException("No element exist");
                }
                return ret;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                this.curPage = -1;
                this.iter = null;
            }
        };
    }

}

