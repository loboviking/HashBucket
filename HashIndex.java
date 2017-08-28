package index;

import global.GlobalConst;
import global.Minibase;
import global.PageId;
import global.RID;
import global.SearchKey;
import global.Page;

/**
 * <h3>Minibase Hash Index</h3>
 * This unclustered index implements static hashing as described on pages 371 to
 * 373 of the textbook (3rd edition).  The index file is a stored as a heapfile.  
 */
public class HashIndex implements GlobalConst {
	
	// Int size in bytes, used to compute offsets 
	protected static final int INT_SIZE = 4;	
	
	// The number of primary bucket pages	
	protected static final int BUCKETS_AMOUNT = 128;

	/** File name of the hash index. */
	protected String fileName;

	/** Page id of the directory. */
	protected PageId headId;
  
	//Log2 of the number of buckets - fixed for this simple index
	protected final int  DEPTH = 7;
	
	
  // --------------------------------------------------------------------------

  /**
   * Opens an index file given its name, or creates a new index file if the name
   * doesn't exist; a null name produces a temporary index file which requires
   * no file library entry and whose pages are freed when there are no more
   * references to it.
   * The file's directory contains the locations of the 128 primary bucket pages.
   * You will need to decide on a structure for the directory.
   * The library entry contains the name of the index file and the pageId of the
   * file's directory.
   */
  public HashIndex(String fileName) {
	  
	  this.fileName = fileName;

	  if (null != fileName)
	  {
		  PageId pageId = Minibase.DiskManager.get_file_entry(fileName); 
		  
		  if (null != pageId)
		  {
			  headId = pageId;
		  }
		  else
		  {
			  CreateEmptyHashIndexFile();
		  }
	  }
	  else // Temporary HashIndex
	  {
		  CreateEmptyHashIndexFile();
	  }	  
  } // public HashIndex(String fileName)

  /**
   * Creates an empty HashIndex file.  Used by HashIndex constructor.
   */
  protected void CreateEmptyHashIndexFile() {

	  // Allocate the index file
	  Page hashDirectoryPage = new Page();
	  headId = Minibase.DiskManager.allocate_page();
	  
	  // Fill the directory page with the 128 primary bucket pages
	  for (int i = 0; i < BUCKETS_AMOUNT; i++)
	  {  
		  // Set up our director structure.  We will just use a simple page structure
		  // where at each index we have an int for the HashBucket page id.
		  // This will fit into one page with 128 indexes with room to spare (will only 512 of 1024 bytes)
		  // We will just initialize the page ids to invalid at first and allocate them as needed
		  hashDirectoryPage.setIntValue(INVALID_PAGEID, i * INT_SIZE);
	  }
	  
	  if (null != fileName && fileName.length() > 0)
	  {
		  // Only add the file entry when we don't have a temporary file
		  Minibase.DiskManager.add_file_entry(fileName, headId);
	  }
	  
	  // Save the the changes for the directory page
	  Minibase.BufferManager.pinPage(headId, hashDirectoryPage, PIN_MEMCPY);
	  Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);

  } // protected void CreateEmptyHashIndexFile()
  
  /**
   * Called by the garbage collector when there are no more references to the
   * object; deletes the index file if it's temporary.
   */
  protected void finalize() throws Throwable {
	  
	  // fileName will not be set when index file is temporary
	  if (fileName.isEmpty())
	  {
		  deleteFile();
	  }

  } // protected void finalize() throws Throwable

   /**
   * Deletes the index file from the database, freeing all of its pages.
   */
  public void deleteFile() {
	  
	  PageId currentPageId = new PageId();
	  Page hashDirectoryPage = new Page();		  
	  
	  // Load the directory page
	  Minibase.BufferManager.pinPage(headId, hashDirectoryPage, PIN_DISKIO);

	  // Traverse the directory, deleting the bucket pages
	  for (int i = 0; i < BUCKETS_AMOUNT; i++)
	  {
		  currentPageId.pid = hashDirectoryPage.getIntValue(i * INT_SIZE);
		  HashBucketPage currentPage = new HashBucketPage();
		  
		  if (INVALID_PAGEID != currentPageId.pid)
		  {
			  // Traverse the bucket, deleting all the extended bucket pages
			  Minibase.BufferManager.pinPage(currentPageId, currentPage, PIN_DISKIO);
			  currentPage.deleteNextPages();

			  // Free the primary bucket page 
			  Minibase.BufferManager.unpinPage(currentPageId, UNPIN_CLEAN);
			  Minibase.BufferManager.freePage(currentPageId);
		  }  
	  }
	  // Remove the entry from the library
	  Minibase.DiskManager.delete_file_entry(fileName);

  } // public void deleteFile()

  /**
   * Inserts a new data entry into the index file.
   * 
   * @throws IllegalArgumentException if the entry is too large
   */
  public void insertEntry(SearchKey key, RID rid) {
	  
	  if (key.getLength() > HashBucketPage.MAX_ENTRY_SIZE)
	  {
		  throw new IllegalArgumentException("Attempted to insert an entry that is too large!");
	  }	  
	  
	  Page directoryPage = new Page();
	  boolean directoryDirty = false;	  
	  
	  // Load the directory page
	  Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO);
	  
	  // Find what primary bucket this needs to go in
	  int hashIndex = key.getHash(DEPTH);
	  
	  // Get the page Id of the primary bucket page at that index
	  PageId primaryBucketId = new PageId(directoryPage.getIntValue(hashIndex * INT_SIZE));
	  HashBucketPage primaryBucketPage = new HashBucketPage();
	  
	  if (INVALID_PAGEID == primaryBucketId.pid)
	  { // No primary bucket page for that index 
	  
		  // so create one
		  primaryBucketId = Minibase.DiskManager.allocate_page();
		  Minibase.BufferManager.pinPage(primaryBucketId, primaryBucketPage, PIN_MEMCPY);
		  
		  // And reference it in the directory
		  directoryPage.setIntValue(primaryBucketId.pid, hashIndex * INT_SIZE);
		  directoryDirty = true;
	  }
	  else
	  { // We have a primary bucket, so load it
		  Minibase.BufferManager.pinPage(primaryBucketId, primaryBucketPage, PIN_DISKIO);  
	  }
	  
	  // Build the entry object
	  DataEntry entry = new DataEntry(key, rid);
	  
	  // Insert the entry in our hash bucket and unpin clean/dirty as appropriate
	  if (primaryBucketPage.insertEntry(entry))
	  {
		  Minibase.BufferManager.unpinPage(primaryBucketId, UNPIN_DIRTY);
	  }
	  else
	  {
		  Minibase.BufferManager.unpinPage(primaryBucketId, UNPIN_CLEAN);
	  }
	  
	  // Unpin the directory page, dirty/clean as appropriate
	  if (directoryDirty)
	  {
		  Minibase.BufferManager.unpinPage(headId, UNPIN_DIRTY);
	  }
	  else
	  {
		  Minibase.BufferManager.unpinPage(headId, UNPIN_CLEAN);
		  
	  }
  } // public void insertEntry(SearchKey key, RID rid)

  /**
   * Deletes the specified data entry from the index file.
   * 
   * @throws IllegalArgumentException if the entry doesn't exist
   */
  public void deleteEntry(SearchKey key, RID rid) {
	  
	  Page directoryPage = new Page();
	  
	  // Load the directory page
	  Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO);
	  
	  // Find what primary bucket this needs to be deleted from
	  int hashIndex = key.getHash(DEPTH);
	  
	  // Get the page Id of the primary bucket page at that index
	  PageId primaryBucketId = new PageId(directoryPage.getIntValue(hashIndex * INT_SIZE));
	  HashBucketPage primaryBucketPage = new HashBucketPage();
	  
	  if (INVALID_PAGEID == primaryBucketId.pid)
	  { // No primary bucket page for that index 
		  throw new IllegalArgumentException("Attempted to delete an entry that did not exist!");
	  }
	  
	  // Load the primary bucket page
	  Minibase.BufferManager.pinPage(primaryBucketId, primaryBucketPage, PIN_DISKIO); 
	  
	  // Build the entry object
	  DataEntry entry = new DataEntry(key, rid);
	  
	  // Delete the entry in our hash bucket and unpin clean/dirty as appropriate
	  if (primaryBucketPage.deleteEntry(entry))
	  {
		  Minibase.BufferManager.unpinPage(primaryBucketId, UNPIN_DIRTY);
	  }
	  else
	  {
		  Minibase.BufferManager.unpinPage(primaryBucketId, UNPIN_CLEAN);
	  }
	  
	  // Unpin our directory page to leave it as we found it
	  Minibase.BufferManager.unpinPage(headId, UNPIN_CLEAN);

  } // public void deleteEntry(SearchKey key, RID rid)

  /**
   * Initiates an equality scan of the index file.
   */
  public HashScan openScan(SearchKey key) {
    return new HashScan(this, key);
  }

  /**
   * Returns the name of the index file.
   */
  public String toString() {
    return fileName;
  }

  /**
   * Prints a high-level view of the directory, namely which buckets are
   * allocated and how many entries are stored in each one. Sample output:
   * 
   * <pre>
   * IX_Customers
   * ------------
   * 0000000 : 35
   * 0000001 : null
   * 0000010 : 27
   * ...
   * 1111111 : 42
   * ------------
   * Total : 1500
   * </pre>
   */
  public void printSummary() {

	  // Print header
	  System.out.println("IX_Customers\n");
	  System.out.println("------------\n");
	  
	  Page directoryPage = new Page();
	  
	  // Load the directory page
	  Minibase.BufferManager.pinPage(headId, directoryPage, PIN_DISKIO);
	  
	  for (int i = 0; i < BUCKETS_AMOUNT; i++)
	  {
		  // Get the page Id of the primary bucket page at that index
		  PageId primaryBucketId = new PageId(directoryPage.getIntValue(i * INT_SIZE));
		  HashBucketPage primaryBucketPage = new HashBucketPage();
		  String numberOfEntries = new String();
		  
		  if (INVALID_PAGEID == primaryBucketId.pid)
		  { // No primary bucket page for that index 
			  numberOfEntries = "null";
		  }
		  else
		  {
			  // Load the primary bucket page for this index
			  Minibase.BufferManager.pinPage(primaryBucketId, primaryBucketPage, PIN_DISKIO); 
			  
			  // Get our entry count for this page and any linked bucket pages
			  numberOfEntries = Integer.toString(primaryBucketPage.countEntries());
			  
			  // Unpin our bucket page to leave it as we found it
			  Minibase.BufferManager.unpinPage(primaryBucketId, UNPIN_CLEAN);	
		  }
		  System.out.println(Integer.toString(i,2) + " : " + numberOfEntries + "\n");
	  }
	  // Unpin our directory page to leave it as we found it
	  Minibase.BufferManager.unpinPage(headId, UNPIN_CLEAN);

  } // public void printSummary()

} // public class HashIndex implements GlobalConst
