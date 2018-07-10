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
    private int numPages;

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
        this.numPages = (int)((f.length() + BufferPool.getPageSize() - 1) / BufferPool.getPageSize());
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
        if (pid.getPageNumber() < 0 || pid.getPageNumber() >= this.numPages) {
            throw new NoSuchElementException(String.format("Page number: %d doesn't exist", pid.getPageNumber()));
        }
        RandomAccessFile fin = null;
        byte[] data = HeapPage.createEmptyPageData();
        try {
            fin = new RandomAccessFile(this.file, "r");
            data = new byte[BufferPool.getPageSize()];
            fin.read(data, pid.getPageNumber() * BufferPool.getPageSize(), BufferPool.getPageSize());
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
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return this.numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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
                        this.iter = ((HeapPage)readPage(new HeapPageId(getId(), this.curPage))).iterator();
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
                        this.iter = ((HeapPage) readPage(new HeapPageId(getId(), this.curPage))).iterator();
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
                this.curPage = 0;
                this.iter = null;
            }

            @Override
            public void close() {
                this.curPage = -1;
                this.iter = null;
            }
        };
    }

}

