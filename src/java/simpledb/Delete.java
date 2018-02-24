package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private OpIterator child;
    private TupleDesc tdFinal;
    boolean alreadyCalled;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
    		this.t = t;
    		this.child = child;
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
					Database.getBufferPool().deleteTuple(this.t, curr);
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
