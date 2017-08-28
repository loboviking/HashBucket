package index;

import global.Minibase;
import global.PageId;

/**
 * An object in this class is a page in a linked list.
 * The entire linked list is a hash table bucket.
 */
class HashBucketPage extends SortedPage {
	

	// This variable is used for deleteEntry to track whether or not we are at the primary bucket page or not
  static int recursionLevel = -1;

  /**
   * Gets the number of entries in this page and later
   * (overflow) pages in the list.
   * <br><br>
   * To find the number of entries in a bucket, apply 
   * countEntries to the primary page of the bucket.
   */
  public int countEntries() {
	  
	  int entryCount = 0;
	  PageId nextPageId = new PageId();
	  nextPageId = this.getNextPage();
	  
	  // If there is another page after this one, add its count (and any subsequent pages' counts to the total).
	  if (INVALID_PAGEID != nextPageId.pid) 
	  {
		  HashBucketPage nextPage = new HashBucketPage();
		  Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);
		  entryCount += nextPage.countEntries();
		  // Leave page unpinned
		  Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN); 
	  } 
	  // Add this page's count to the total
	  entryCount += getEntryCount();
	  
	  return entryCount;

  } // public int countEntries()

  /**
   * Inserts a new data entry into this page. If there is no room
   * on this page, recursively inserts in later pages of the list.  
   * If necessary, creates a new page at the end of the list.
   * Does not worry about keeping order between entries in different pages.
   * <br><br>
   * To insert a data entry into a bucket, apply insertEntry to the
   * primary page of the bucket.
   * 
   * @return true if inserting made this page dirty, false otherwise
   */
  public boolean insertEntry(DataEntry entry) {
	  
	  boolean dirty = false;
	  
	  try 
	  {
		  // Try to insert the entry into this page, if it throws an IllegalStateException, then
		  // there is not enough room on this page.
		  dirty = super.insertEntry(entry);
	  }
	  catch (IllegalStateException e)
	  {
		  // This page does not have enough space, we must insert into the next page on the list

		  PageId nextPageId = new PageId();
		  nextPageId = this.getNextPage();
		  HashBucketPage nextPage = new HashBucketPage();
		  
		  if (INVALID_PAGEID == nextPageId.pid) 	  
		  {
			  // There is no page after this one so we must create a new HashBucketPage 
			  
			  // Create the new HashBucketPage
			  nextPageId = Minibase.DiskManager.allocate_page();
			  
			  // Set this page's next page to the one we just created
			  this.setNextPage(nextPageId);
			  
			  // This page is now dirty.  Will also indicate that we need to UNPIN_DIRTY the next page
			  dirty = true;			  
			  
			  // Initialize the next page that we just created
			  Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_MEMCPY);
		  }
		  else
		  {   // Load the next page
			  Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);
		  }
		  
		  // Insert the entry into the next page
		  
		  if (nextPage.insertEntry(entry) || true == dirty)
		  {
			  // Leave page unpinned dirty (page was dirtied by call to insertEntry or new page creation)
			  Minibase.BufferManager.unpinPage(nextPageId, UNPIN_DIRTY);
		  }
		  else
		  {
			  // Leave page unpinned clean (page was not dirtied by call to insertEntry, or by new page creation)
			  Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN);
		  }	  
	  }

	  return dirty;

  } // public boolean insertEntry(DataEntry entry)

  /**
   * Deletes a data entry from this page.  If a page in the list 
   * (not the primary page) becomes empty, it is deleted from the list.
   * 
   * To delete a data entry from a bucket, apply deleteEntry to the
   * primary page of the bucket.
   * 
   * 
   * @return true if deleting made this page dirty, false otherwise
   * @throws IllegalArgumentException if the entry is not in the list.
   */
  public boolean deleteEntry(DataEntry entry) {
	  
	  boolean dirty = false;
	  
	  try
	  {
		  // Try to delete the entry from this page, if it throws an IllegalArgumentException, then
		  // the entry is not on this page
		  dirty = super.deleteEntry(entry);	
	  }
	  catch (IllegalArgumentException e)
	  {
		  // Entry is not on this page so attempt to delete from the next page
		  
		  PageId nextPageId = new PageId();
		  nextPageId = this.getNextPage();
		  
		  if (INVALID_PAGEID == nextPageId.pid) 	  
		  {
			  // There is no page after this one so we just throw an IllegalArgumentException
			  // since the entry is nowhere on the list.
			  // Note that this exception will not be caught by deleteEntry (intentional)
			  // because it is not called from within the try block.
			  throw new IllegalArgumentException("deleteEntry failed.  Entry not found!");
		  }
		  
		  // Delete the entry from the next page
	
		  HashBucketPage nextPage = new HashBucketPage();
		  Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);
		  
		  if (nextPage.deleteEntry(entry))
		  {// nextPage ended up dirty after deleteEntry
			  
			  // If this left the next page empty, then we need to delete that page and update our next page Id
			  if (0 == nextPage.getEntryCount())
			  {
				  this.setNextPage(nextPage.getNextPage());
				  Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN);				  
				  Minibase.BufferManager.freePage(nextPageId);
				  dirty = true;
			  }
			  else
			  {
				  // Leave page unpinned dirty (page was dirtied by call to deleteEntry)
				  Minibase.BufferManager.unpinPage(nextPageId, UNPIN_DIRTY);
			  }
		  }// nextPage ended up clean after deleteEntry
		  else
		  {
			  // Leave page unpinned clean (page was not dirtied by call to deleteEntry)
			  Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN);
		  }	 		    
	  }
	  return dirty;

  } // public boolean deleteEntry(DataEntry entry)

  /**
   * Deletes the next pages after this one.  
   * 
   * To delete all the bucket pages after the primary , apply deleteNextPages to the
   * primary page of the bucket.
   * 
   */
  public void deleteNextPages() {
	  
	  HashBucketPage nextPage = new HashBucketPage();
	  PageId nextPageId = this.getNextPage();
	  
	  if (INVALID_PAGEID != nextPageId.pid)
	  {
		  // We have another page after this one, so check that page to see if it has another page
		  Minibase.BufferManager.pinPage(nextPageId, nextPage, PIN_DISKIO);
		  
		  if (INVALID_PAGEID != nextPage.getNextPage().pid)
		  {
			  // There is another page after the next, so call deleteNextPages on the next page
			  nextPage.deleteNextPages();
		  }
		  // Downstream pages (if any) should have been deleted by now so go ahead and delete the next page
		  Minibase.BufferManager.unpinPage(nextPageId, UNPIN_CLEAN);
		  Minibase.BufferManager.freePage(nextPageId);  
	  } 		    

  } // public boolean deleteEntry(DataEntry entry)
  
} // class HashBucketPage extends SortedPage
