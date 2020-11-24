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
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.tupleDesc = td;
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
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
            byte[] buffer = new byte[BufferPool.getPageSize()];
            randomAccessFile.seek(pid.getPageNumber() * BufferPool.getPageSize());
            if (randomAccessFile.read(buffer) == -1) {
                throw new IllegalArgumentException();
            }
            randomAccessFile.close();
            return new HeapPage((HeapPageId)pid, buffer);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("readPage :"+pid.getPageNumber()+","+pid.getTableId()+","+numPages()+","+file.length());
            throw new IllegalArgumentException(e.getMessage());
        }
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
        return (int)(this.file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {

            private int numPage = numPages();
            private int pid = 0;
            private BufferPool bufferPool = Database.getBufferPool();
            private HeapPage heapPage;
            private Iterator<Tuple> tupleIterator;
            private boolean isOpen = false;
            private boolean hasNext = false;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                pid = 0;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!isOpen){
                    return false;
                }

                if(tupleIterator != null){
                    if(tupleIterator.hasNext()){
                        hasNext = true;
                        return true;
                    }else {
                        pid++;
                    }
                }

                while (pid < numPages()){
                    heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(),pid), Permissions.READ_ONLY);
                    tupleIterator = heapPage.iterator();
                    if(tupleIterator.hasNext()){
                        hasNext = true;
                        return true;
                    }
                    pid++;
                }
                hasNext = false;

                return false;
            }


            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!isOpen || !hasNext){
                    throw new NoSuchElementException();
                }
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pid = 0;
                isOpen = false;
                hasNext = false;
                heapPage = null;
                tupleIterator = null;
            }
        };
    }

}

