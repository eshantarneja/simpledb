package simpledb;

import java.util.*;

import simpledb.Predicate.Op;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private TupleDesc td1;
    private TupleDesc td2;
    private Tuple t1_pointer;
    private Tuple t2_pointer;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
    		this.p = p;
    		this.child1 = child1;
    		this.child2 = child2;
    		this.td1 = child1.getTupleDesc();
    		this.td2 = child2.getTupleDesc();
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return td1.getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return td2.getFieldName(p.getField2());
    }

    
   /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    		return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }
    	
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
		super.open();    
    		this.child1.open();
    		this.child2.open();
    }

    public void close() {
        // some code goes here
    		child1.close();
    		child2.close();
    		super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    		//OpIterator[] child_list = getChildren();
    		this.child1.rewind();
    		this.child2.rewind();
        }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    		Tuple next_match;
    		TupleDesc next_match_td;
    		    	
    		while (child1.hasNext() == true || t1_pointer != null) {
    			
    				if (t1_pointer == null) {
    					t1_pointer = child1.next();
    					child2.rewind();
    				}
    			  
    			while (child2.hasNext() == true) {
    			
    				t2_pointer = child2.next();
    				    				
    				if (p.filter(t1_pointer, t2_pointer) == true) {
    					
    					next_match_td = getTupleDesc();
    					
    					next_match = new Tuple(next_match_td);
    					    					
    					int total_counter = 0;
    					int field_pointer = 0;
    					
    					while (total_counter < td1.getSize()) {
    						
    						Field temp2 = t1_pointer.getField(field_pointer);
    						
    						if (temp2 != null) {
    						
    							next_match.setField(total_counter, temp2);
    							field_pointer++; total_counter++;
    						}
    						
    						else {break;}
    					}
    					
    					field_pointer = 0;
    					
    					while (total_counter < td1.getSize() + td2.getSize()) {
    						
    						Field temp = t2_pointer.getField(field_pointer);
    						
    						if (temp != null) {
    						
    						next_match.setField(total_counter, t2_pointer.getField(field_pointer));
    						field_pointer++; total_counter++;
    						
    						}
    						
    						else {break;}
    					}
    					    	
    					return next_match;
    					    					
    				} 
    			} t1_pointer = null;
    			
    		}
    		
    		return null;
    }
    		
    
    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child1, child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    		children[0] = child1;
    		children[1] = child2;
    }
}