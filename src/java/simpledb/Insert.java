package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator opIterator;
    private int tableId;
    private OpIterator[] opIterators;
    private int count;
    private TupleDesc tupleDesc;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.transactionId = t;
        this.opIterator = child;
        this.tableId = tableId;
        this.opIterators = new OpIterator[]{child};
        this.count = 0;
        this.tupleDesc = Utility.getTupleDesc(1);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.opIterator.open();
        super.open();
    }

    public void close() {
        // some code goes here
        this.opIterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.opIterator.rewind();

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
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while(opIterator.hasNext()){
            try{
                Tuple tuple = opIterator.next();
                Database.getBufferPool().insertTuple(transactionId,tableId,tuple);
                count++;
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
        Tuple tuple = new Tuple(Utility.getTupleDesc(1));
        tuple.setField(0,new IntField(count));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return this.opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.opIterators = children;
    }
}
