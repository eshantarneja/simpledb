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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	
	File file;
	TupleDesc tDesk;
	int hId;
	int pSize;
	
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    		file = f;
    		tDesk = td;
    		hId = f.getAbsolutePath().hashCode();
    		pSize = BufferPool.getPageSize(); 
    			
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tDesk;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here  
//    		int pSize = BufferPool.getPageSize(); 
    		byte[] temp = new byte[pSize];
        RandomAccessFile rFile;
        int offset = pid.getPageNumber() * pSize;
        
		try {
			rFile = new RandomAccessFile(file, "r");
			rFile.seek(offset);
			rFile.read(temp, 0, pSize);
			rFile.close();
			Page ret = (Page)(new HeapPage((HeapPageId)pid, temp));
			return ret;
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		} 
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    		RandomAccessFile rFile = new RandomAccessFile(this.file,"rw");
		int off = BufferPool.getPageSize() * this.numPages();
		rFile.seek(off);
		rFile.write(page.getPageData(), 0, BufferPool.getPageSize());
		rFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
//		int pSize = BufferPool.getPageSize(); 
		int fSize = (int) this.file.length();
		return (int)Math.ceil(fSize/pSize);	
    }
    private HeapPage EmptyPage(TransactionId tid) throws TransactionAbortedException, DbException, IOException {
    		for(int i = 0; i < this.numPages(); i++) {
			int tabId = HeapFile.this.getId();
			PageId pid = new HeapPageId(tabId, i);
			HeapPage currP =(HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
			if(currP.getNumEmptySlots() > 0) {
				return currP;
			}
    		}
    		return null;  		
    	
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    		ArrayList<Page> pages = new ArrayList<Page>();
    		// find empty page
    		HeapPage pge = EmptyPage(tid);
    		if(pge == null) {
    			int tabId = HeapFile.this.getId();
    			HeapPageId nPid = new HeapPageId(tabId, this.numPages());
    			HeapPage newPage = new HeapPage(nPid, HeapPage.createEmptyPageData());
    			newPage.insertTuple(t);
    			this.writePage(newPage);
//    			RandomAccessFile rFile = new RandomAccessFile(this.file,"rw");
//    			int off = BufferPool.getPageSize() * this.numPages();
//    			rFile.seek(off);
//    			rFile.write(newPage.getPageData(), 0, BufferPool.getPageSize());
//    			rFile.close();
    			pages.add(newPage);
    		}
    		else {
    			pge.insertTuple(t); 
    			pages.add(pge);
    		}
    		return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    		ArrayList<Page> pages = new ArrayList<Page>(); 		
		PageId pid = t.getRecordId().getPageId();
		HeapPage currP;
		currP = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		currP.deleteTuple(t);
		pages.add(currP);

		return pages;
		
    		
        // some code goes here
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here

    		return new DbFileIterator() {
        			//locals
        			Page currPage;
        			HeapPageId currPageId;
        			Iterator<Tuple> currPageItr;
        			int tabId;
        			int currPageNum;
        			int nPages = HeapFile.this.numPages();
        			boolean open;
        			
        			
    				@Override
    				public void rewind() throws DbException, TransactionAbortedException {
    					if(open) {
        					this.open();	
    					}

    				}
    				
    				@Override
    				public void open() throws DbException, TransactionAbortedException{
    					open = true;
    					currPageNum = 0;
    					tabId = HeapFile.this.getId();
    					currPageId = new HeapPageId(tabId, currPageNum);
					currPage = Database.getBufferPool().getPage(new TransactionId(), currPageId, null);
    					currPageItr = ((HeapPage)currPage).iterator();   					
    				}
    				
    				@Override
    				public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    					if(open) {
    						if(hasNext()) {
        						return currPageItr.next();
        					}
        					else {
        						throw new NoSuchElementException();
        					}	
    					}
    					else {
    						throw new NoSuchElementException();
    					}
    				}
    				
    				@Override
    				public void close() {
    					open = false;
    				}

				@Override
				public boolean hasNext() throws DbException, TransactionAbortedException {
						// TODO Auto-generated method stub
						if(open) {
							if(currPageItr.hasNext() == true) {
								return true;
							}
							else {
																
								if(currPageNum < (nPages-1)) {
									currPageNum++;
			    						currPageId = new HeapPageId(tabId, currPageNum);
									currPage = Database.getBufferPool().getPage(new TransactionId(), currPageId, null);
			    	   					currPageItr = ((HeapPage)currPage).iterator();  
									return hasNext();
								}
								return false;
							}							
						}
						else {
//							throw new NoSuchElementException();
							return false;
						}
				}
    		};
    }

}
