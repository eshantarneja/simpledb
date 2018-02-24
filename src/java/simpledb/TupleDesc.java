package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
	
	/*Added global var arr*/
	ArrayList<TDItem> arr = new ArrayList<TDItem>(); 

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
    	return iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here  
    	for (int counter = 0; counter < typeAr.length; counter++){  
    		TDItem to_add = new TDItem(typeAr[counter],fieldAr[counter]);
    		arr.add(to_add);
    		}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	for (int counter = 0; counter < typeAr.length; counter++){  
    		TDItem to_add = new TDItem(typeAr[counter],null);
    		arr.add(to_add);
    		}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
    	return arr.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    		//System.out.println(arr.size());
    		//System.out.println(i);
    	
    		if (i < arr.size()){
    		return arr.get(i).fieldName;}
    		
    		else {throw new NoSuchElementException();}
    		
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	return arr.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	
    for(int i= 0; i <this.numFields();i++) {
    		String t = this.getFieldName(i);
    		if(t != null && t.equals(name)){
    			return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int s = 0;
        Iterator<TDItem> itr = arr.iterator();
        while(itr.hasNext()){
        	s += (int)itr.next().fieldType.getLen();
        }
        return s;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here

    		Type[] type_list = new Type[td1.numFields()+td2.numFields()];    		
    		String[] string_list = new String[td1.numFields()+td2.numFields()];
    		
    		for (int x = 0; x < td1.numFields(); x++) {
    			type_list[x] = td1.getFieldType(x);
    			string_list[x] = td1.getFieldName(x);
    		}
    		
    		for (int y = td1.numFields(); y < td1.numFields() + td2.numFields(); y++) {
    			type_list[y] = td2.getFieldType(y - td1.numFields());
    			string_list[y] = td2.getFieldName(y - td1.numFields());
    		}

    		TupleDesc to_ret = new TupleDesc(type_list, string_list);
    		
    		return to_ret;
    	
    }


    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	//is o a TupleDesc
    	if(!(o instanceof TupleDesc)){return false;}
	
		TupleDesc test = (TupleDesc)o;
		//are they same size
		if(arr.size() != test.arr.size()){
			return false;
		}
		
		Iterator<TDItem> itr1 = arr.iterator();
		Iterator<TDItem> itr2 = test.arr.iterator();
		
		//checking if types are equal
		while(itr1.hasNext()){
			if((Type)itr1.next().fieldType != (Type)itr2.next().fieldType){
				return false;
			}
		}
		return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	Iterator<TDItem> itr = arr.iterator();
    	String a = "";
    	while(itr.hasNext()){
    		TDItem curr = (TDItem)itr.next();
    		a = a + curr.fieldType.toString() + ":" + curr.fieldName + ", ";
    	}
        return a;
    }
}