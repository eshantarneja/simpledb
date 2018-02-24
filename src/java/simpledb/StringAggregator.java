package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<String, Integer> category_count = new HashMap<String, Integer>();
    private int ungrouped_count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		this.gbfield = gbfield;
    		this.afield = afield;
    		this.gbfieldtype = gbfieldtype;
    		this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	
    		//if we don't need to group we are just counting
    		if (gbfield == NO_GROUPING) {
    			ungrouped_count++;
    		}
    	
    		else {
    			String key_val;
    			
    			//For integer group by types
    			if (gbfieldtype == Type.INT_TYPE) {
    				IntField int_field = (IntField)tup.getField(gbfield);
    				key_val = Integer.toString(int_field.getValue());}
    			//For string group by types
    			else {
    				StringField string_field = (StringField)tup.getField(gbfield);
    				key_val = string_field.getValue();
    			}
    			//if there's been an instance of that category already
    			if (category_count.containsKey(key_val) == true) {
    				int curr_count = category_count.get(key_val);
    				category_count.put(key_val, curr_count+1);
    			}
    			//if there hasn't been an instance of that category yet
    			else {
    				category_count.put(key_val, 1);
    			}		
    		}		
    	}
    		

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();

    		if (gbfield == NO_GROUPING) {
    			
    			Type[] typeAr = new Type[1];
    			typeAr[0] = Type.INT_TYPE;
    			String[] fieldAr = new String[1];
    			fieldAr[0] = "Count";
    		
    			TupleDesc td = new TupleDesc(typeAr, fieldAr);
    			
    			Tuple to_ret = new Tuple(td);
    			    			
    			to_ret.setField(0, new IntField(ungrouped_count));
    			
    			//ArrayList<Tuple> tuples;
    			tuples.add(to_ret);
    			
    			//return new OpIterator(td, tuples);
    			return new TupleIterator(td, tuples);
    		}
    		
    		else {
    			Type[] typeAr = new Type[2];
    			String[] fieldAr = new String[2];
    			typeAr[1] = Type.INT_TYPE;
    			typeAr[0] = gbfieldtype;
    			TupleDesc td = new TupleDesc(typeAr, fieldAr);
    			//Tuple to_ret = new Tuple(td);
    			//ArrayList<Tuple> tuples;
    			
    			for (String cat_instance : category_count.keySet()) {
    				
        			Tuple to_ret = new Tuple(td);
    				
    				int add_val;
    				
    				if (gbfieldtype == Type.INT_TYPE) {
    					add_val = Integer.parseInt(cat_instance);
    					to_ret.setField(0, new IntField(add_val));
    				}
    				
    				else {
    					to_ret.setField(0,  new StringField(cat_instance, 100));
    				}
    				
    				
    				to_ret.setField(1,  new IntField(category_count.get(cat_instance)));
    				
    				tuples.add(to_ret);
    				
    			}
    			
    			return new TupleIterator(td, tuples);
    		}	    	
    }
}