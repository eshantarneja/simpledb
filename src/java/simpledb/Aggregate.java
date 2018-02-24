package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private TupleDesc td;
    private int agg_type = -1; //0 --> string; 1 --> integer
    private Aggregator use;
    private Type[] typeAr;
    private String[] fieldAr;
    private OpIterator agg_results;
    private OpIterator[] children = new OpIterator[1];
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
    		this.child = child;
    		this.afield = afield;
    		this.gfield = gfield;
    		this.aop = aop;
    		this.td = child.getTupleDesc();
    		
    		if (td.getFieldType(afield) == Type.INT_TYPE) {
    			agg_type = 1;
    		}
    		
    		if (td.getFieldType(afield) == Type.STRING_TYPE) {
    			agg_type = 0;
    		}
    		
//    		if (td.getFieldType(gfield) == Type.STRING_TYPE) {
//    			gb_type = 0;
//    		}
//    		
//    		if (td.getFieldType(gfield) == Type.INT_TYPE) {
//    			gb_type = 1;
//    		}
  
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
    		if (gfield == -1) {
    			return simpledb.Aggregator.NO_GROUPING;
    		}
    		else {
    			return gfield;
    		}
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
    		if (gfield == -1) {return null;}
    		
    		else {
    			return td.getFieldName(gfield);
    		}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
    		return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
    		return td.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
    		return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    		return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
    	
		super.open();
    	
    		if (agg_type == 0 && gfield == -1) {//string, no groups
    			use = new StringAggregator(Aggregator.NO_GROUPING, null, afield, aop);
    		}
    		
    		else if (agg_type == 0) {//string, groups
    			use = new StringAggregator(gfield, td.getFieldType(gfield), afield, aop);
    		}
    		
    		else if (agg_type == 1 && gfield == -1) {//int, no groups
    			use = new IntegerAggregator(Aggregator.NO_GROUPING, null, afield, aop);
    		}
    		
    		else {//int, groups
    			use = new IntegerAggregator(gfield, td.getFieldType(gfield), afield, aop);
    		}
    		
    		child.open();
    		
    		while (child.hasNext() == true) {
    			
    			use.mergeTupleIntoGroup(child.next());
    			
    		}
    		
    		agg_results = use.iterator();
    		
    		agg_results.open();
    		    		
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
 		
    	
    	if (agg_results.hasNext() == true) {
    		return agg_results.next();
    	}
    		else {return null;}
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
	    		
    		agg_results.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
    		if (gfield == -1) {//no group by
    			
    			typeAr = new Type[1];
    			fieldAr = new String[1];
    			
    			if (agg_type == 0) {//string
    				typeAr[0] = Type.STRING_TYPE;
    				fieldAr[0] = aggregateFieldName();
    			}
    			
    			else {//int
    				typeAr[0] = Type.INT_TYPE;
    				fieldAr[0] = aggregateFieldName();
    			}
    			
    		}
    		
    		else {
    			
    			typeAr = new Type[2];
    			fieldAr = new String[2];
    			
    			if (agg_type == 0) {//string
				typeAr[0] = Type.STRING_TYPE;
				typeAr[1] = Type.INT_TYPE;			
			}
    			
    			else {//int
    				typeAr[0] = Type.INT_TYPE;
    				typeAr[1] = Type.INT_TYPE;	
    			}
    			
    			    			
    			fieldAr[1] = aggregateFieldName();
    			fieldAr[0] = td.getFieldName(gfield);
    			
    		}
    		
    		TupleDesc to_ret = new TupleDesc(typeAr, fieldAr);
    		return to_ret;
    		
    }

    public void close() {
	// some code goes here
		super.close();
    		child.close();
    		agg_results.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
    		return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
    		children[0] = child;
    }
    
}