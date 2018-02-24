package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
	
	public int table_id;
	public int page_number;

    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
    		table_id = tableId;
    		page_number = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return table_id;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return page_number;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
//    		String hash_code;
//    		String pt1 = String.valueOf(table_id);
//    		String pt2 = String.valueOf(page_number);
//    		hash_code = pt1 + pt2; 
//    		int to_ret = Integer.parseInt(hash_code);
//    		return to_ret;
    			String temp = "" + table_id + page_number;
    			int hash = temp.hashCode();
    			return hash;
    			
    			
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
    		if (!(o instanceof HeapPageId)) {return false;}
    		HeapPageId to_check = (HeapPageId)o;
    		if (to_check.page_number == page_number && to_check.table_id == table_id) {
    			return true;
    		}
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}