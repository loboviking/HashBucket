package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;
import global.Page;

/**
 * A HashScan retrieves all records with a given key (via the RIDs of the records).  
 * It is created only through the function openScan() in the HashIndex class. 
 */
public class HashScan implements GlobalConst {

  /** The search key to scan for. */
  protected SearchKey key;

  /** Id of HashBucketPage being scanned. */
  protected PageId curPageId;

  /** HashBucketPage being scanned. */
  protected HashBucketPage curPage;

  /** Current slot to scan from. */
  protected int curSlot;

  // --------------------------------------------------------------------------

  /**
   * Constructs an equality scan by initializing the iterator state.
   */
  protected HashScan(HashIndex index, SearchKey key) {
	  
	  // Initialize data fields
	  this.key = key;
	  curPage = null;
	  curSlot = -1;
	  curPageId = new PageId();

	  // Load the directory page and get the bucket page for this search key
	  Page hashDirectoryPage = new Page();
	  Minibase.BufferManager.pinPage(index.headId, hashDirectoryPage, PIN_DISKIO);
	  
	  // Get our primary bucket page id
	  int offset = key.getHash(index.DEPTH) * HashIndex.INT_SIZE;
	  curPageId.pid = hashDirectoryPage.getIntValue(offset);
	  
	  // Unpin our directory page since we have the bucket
	  Minibase.BufferManager.unpinPage(index.headId, UNPIN_CLEAN);

  } // protected HashScan(HashIndex index, SearchKey key)

  /**
   * Called by the garbage collector when there are no more references to the
   * object; closes the scan if it's still open.
   */
  protected void finalize() throws Throwable {

	    // Close the scan, if open
	    if (curPage != null) {
	      close();
	    }	  

  } // protected void finalize() throws Throwable

  /**
   * Closes the index scan, releasing any pinned pages.
   */
  public void close() {

	    // Unpin the current page when applicable
	    if (curPage != null) {
	      Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
	      curPage = null;
	    }

	    // invalidate the other fields
		this.key = null;
		curPage = null;
		curPageId = null;
		curSlot = -1;

  } // public void close()

   /**
   * Gets the next entry's RID in the index scan.
   * 
   * @throws IllegalStateException if the scan has no more entries
   * NOTE: the test code did not appear to expect an IllegalStateException in this case.  I returned null instead.
   */
  public RID getNext() {
	  
	  // If we have no page to start with, then just return null
	  if (INVALID_PAGEID == curPageId.pid)
	  {
		  return null;
	  }
	  
	  int nextSlot = -1;
	  
	  if (null == curPage) // Scan is just starting, we have not loaded curPage
	  {
		  // Load curPage
		  curPage = new HashBucketPage();
		  Minibase.BufferManager.pinPage(curPageId, curPage, PIN_DISKIO);
	  }
	  // Check for the key in the current page page
	  nextSlot = curPage.nextEntry(key, curSlot);
	  
	  if (-1 == nextSlot)
	  { // We didn't find an entry for this key on this page, so it is time to check the next page (if there is one)
		  
		  PageId nextPageId = curPage.getNextPage();
		  
		  if (INVALID_PAGEID != nextPageId.pid)
		  { // We have another page to check
			  
			  // Moving on to a new page so unpin the old one
			  Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
			  
			  // Set up the state for the another call to getNext
			  curPage = null;
			  curPageId = nextPageId;
			  curSlot = -1;
			  
			  // Make the recursive call
			   return getNext();
		  }
		  else
		  { // There is no other page to check so just unpin the current page, clear the fields and return null
			  Minibase.BufferManager.unpinPage(curPageId, UNPIN_CLEAN);
			  this.key = null;
			  curPage = null;
			  curPageId = null;
			  curSlot = -1;	
			  
			  return null;
		  }
	  }
	  else
	  { // We found an entry for this key on this page
		  
		  // So we just set the slot value and return the RID
		  curSlot = nextSlot;
		  DataEntry dataEntry = curPage.getEntryAt(curSlot);
		  
		  return dataEntry.rid;
	  }

  } // public RID getNext()

} // public class HashScan implements GlobalConst
