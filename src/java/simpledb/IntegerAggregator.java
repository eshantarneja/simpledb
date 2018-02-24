package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    
    private HashMap<String, Integer> agg_hold = new HashMap<String, Integer>();
    private HashMap<String, Integer> counts = new HashMap<String, Integer>();
    //private int ungrouped_toret;
    //private int ungrouped_count;
    private String key_val;
        
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    		this.gbfield = gbfield;
    		this.afield = afield;
    		this.gbfieldtype = gbfieldtype;
    		this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	
    		//Assign hash key based on type & group boolean
		if (gbfield == NO_GROUPING) {key_val = "ungrouped";}
		else if (gbfield != NO_GROUPING && gbfieldtype == Type.INT_TYPE){
			IntField int_field = (IntField)tup.getField(gbfield);
			key_val = Integer.toString(int_field.getValue());}
		else {
			StringField string_field = (StringField)tup.getField(gbfield);
			key_val = string_field.getValue();}
		
		IntField int_field = (IntField)tup.getField(afield);

		if (what == Op.MAX) {
			if (agg_hold.containsKey(key_val) == false) {
				agg_hold.put(key_val, int_field.getValue());
				counts.put(key_val, 1);
			}
			else {
				if (int_field.getValue() > agg_hold.get(key_val)) {
					agg_hold.put(key_val, int_field.getValue());}
				int to_add_2 = counts.get(key_val);
				//counts.put(key_val, to_add_2+1);
				}
			}
		
		else if (what == Op.MIN) {
			if (agg_hold.containsKey(key_val) == false) {
				agg_hold.put(key_val, int_field.getValue());
				counts.put(key_val, 1);
			}
			else {
				if (int_field.getValue() < agg_hold.get(key_val)) {
					agg_hold.put(key_val, int_field.getValue());}
				int to_add_2 = counts.get(key_val);
				//counts.put(key_val, to_add_2+1);
				}
			}
		
		else if (what == Op.SUM) {
			if (agg_hold.containsKey(key_val) == false) {
				agg_hold.put(key_val, int_field.getValue());
				counts.put(key_val, 1);
			}
			else {
				int to_add = int_field.getValue() + agg_hold.get(key_val);
				agg_hold.put(key_val, to_add);
				int to_add_2 = counts.get(key_val);
				//counts.put(key_val, to_add_2+1);
				}
			}
		
		else if (what == Op.COUNT) {
			if (agg_hold.containsKey(key_val) == false) {
				agg_hold.put(key_val, 1);
				counts.put(key_val, 1);
			}
			else {
				int temp = agg_hold.get(key_val);
				agg_hold.put(key_val, temp+1);
				int to_add_2 = counts.get(key_val);
				//counts.put(key_val, to_add_2+1);
				}
			}
		
		else if (what == Op.AVG) {
			if (agg_hold.containsKey(key_val) == false) {
				agg_hold.put(key_val, int_field.getValue());
				counts.put(key_val, 1);
			}
			else {
				int to_add = int_field.getValue() + agg_hold.get(key_val);
				agg_hold.put(key_val, to_add);
				int to_add_2 = counts.get(key_val);
				counts.put(key_val, to_add_2+1);
				}
			}
}
			    	
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
    	
        // some code goes here
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		
		if (gbfield == NO_GROUPING) {
						
			Type[] typeAr = new Type[1];
			typeAr[0] = Type.INT_TYPE;
			String[] fieldAr = new String[1];
			fieldAr[0] = "Result";
		
			TupleDesc td = new TupleDesc(typeAr, fieldAr);
			
			Tuple to_ret = new Tuple(td);
						    			
			if (what == Op.AVG) {
				int average = agg_hold.get(key_val)/counts.get(key_val);
				to_ret.setField(0, new IntField(average));
			}
			
			else {
			to_ret.setField(0, new IntField(agg_hold.get(key_val)));
			}
			
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
			boolean avg_ticker = false;
			
			if(what == Op.AVG) {
			avg_ticker = true;}
						
			for (String cat_instance : agg_hold.keySet()) {
												
    			Tuple to_ret = new Tuple(td);
				
				int add_val;
				
				if (gbfieldtype == Type.INT_TYPE) {
					add_val = Integer.parseInt(cat_instance);
					to_ret.setField(0, new IntField(add_val));
				}
				
				else {
					to_ret.setField(0,  new StringField(cat_instance, 100));
				}
				

				if (avg_ticker == true) {
					int average = agg_hold.get(cat_instance)/counts.get(cat_instance);
					to_ret.setField(1, new IntField(average));
				}
				
				else {
				
				to_ret.setField(1,  new IntField(agg_hold.get(cat_instance)));
				}
				
				tuples.add(to_ret);

			}
			
			return new TupleIterator(td, tuples);
		}	

    }

}