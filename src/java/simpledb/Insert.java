package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private int tableId;
    private TupleDesc tdFinal;
	boolean alreadyCalled;
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
    		this.child = child;
    		this.t = t;
    		this.tableId = tableId;
    		Type[] type = {Type.INT_TYPE};
    		String[] count = {"count"};
    		tdFinal = new TupleDesc(type,count);
    		this.alreadyCalled = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    		return this.tdFinal;

    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    		super.open();
    		this.child.open();
    }

    public void close() {
        // some code goes here
    		this.child.close();
    		super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		this.child.rewind();
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
    	
    		//called more than once check    	
    		if(alreadyCalled) {
    			return null;
    		}
    		else {
    		alreadyCalled = true;
    		Tuple toReturn = new Tuple(tdFinal);
    		int tCount = 0;
    		while(child.hasNext()) {
    			Tuple curr = child.next();
    			try {
    					Database.getBufferPool().insertTuple(this.t, this.tableId, curr);
    					tCount++;
    					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    		toReturn.setField(0, new IntField(tCount));
        return toReturn;
    		}
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
    		return new OpIterator[] {this.child};
        
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    		this.child = children[0];
    }
}
