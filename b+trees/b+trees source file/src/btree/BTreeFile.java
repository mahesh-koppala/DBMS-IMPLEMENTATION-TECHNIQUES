/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;
/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used
	 * to drive a visualization tool that shows the inner workings of the b-tree
	 * during its operations.
	 *
	 * @param filename
	 *            input parameter. The trace file name
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this
	 *         btree file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno)
			throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 *
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException,
			PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; -
		 * headerPage, headerPageId valid and pinned - dbname contains a copy of
		 * the name of the database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException,
			IteratorException, PinPageException, ConstructPageException,
			UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page,
					headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
					.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException,
			PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException,
			KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException,
			UnpinPageException, PinPageException, NodeNotMatchException,
			ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException,
			IOException

	{
	//checking whether the header page id exists or not
		if(headerPage.get_rootId().pid==-1) // if headerpage does not exist
		{ 
			BTLeafPage newRootPage;
			PageId newRootPageID, emptyID = null;

			newRootPage = new BTLeafPage(headerPage.get_keyType());  //Creating root page or object instantiation for the leafpage class
			newRootPageID =newRootPage.getCurPage();			//get the page id of the root created
			newRootPage.setNextPage(new PageId(-1));	// setting the next page pointer to null
			newRootPage.setPrevPage(new PageId(-1));  	// setting the previous page pointer to null
			newRootPage.insertRecord(key, rid);       	// inserting the record into the created page
System.out.println(key);
			unpinPage(newRootPageID, true);					//	unpin the page
			updateHeader(newRootPageID);						// updating the header of the page after the record is inserted
		}
		else
		{  //if headerpage or a rootpage already exists
			KeyDataEntry newRootEntry = null;
			newRootEntry = _insert(key,rid, headerPage.get_rootId());	//creating instance to catch the return statement from _insert() 
			if(newRootEntry!=null)				// split has occured
			{
				BTIndexPage newRootIndexPage = new BTIndexPage(NodeType.INDEX);		//creating a new index page
				IndexData indexRecord = (IndexData) newRootEntry.data;
				newRootIndexPage.insertKey(newRootEntry.key, indexRecord.getData());  //inserting record into the new index page
				newRootIndexPage.setPrevPage(headerPage.get_rootId());		//setting the previous page pointer of the new root to the old root
				unpinPage(newRootIndexPage.getCurPage(), true);				//unpin the newroot(index node)
				updateHeader(newRootIndexPage.getCurPage());				//updating the header of the newroot(index node)
			}
		}
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException,
			LeafDeleteException, ConstructPageException, DeleteRecException,
			IndexSearchException, UnpinPageException, LeafInsertRecException,
			ConvertException, IteratorException, IndexInsertRecException,
			KeyNotMatchException, NodeNotMatchException, InsertException

	{
		BTSortedPage currentPage =  new BTSortedPage(currentPageId, headerPage.get_keyType()); 	//creating instance of a BTSortedPage
		if(currentPage.getType() == NodeType.LEAF)			// if current page is a leaf type page
		{
			BTLeafPage currentLeafPage = new BTLeafPage(currentPageId, headerPage.get_keyType());	//create a leaf page
			if(currentLeafPage.available_space() >= BT.getKeyDataLength(key, currentLeafPage.getType()))	//current leaf page has space for entries
			{
				currentLeafPage.insertRecord(key,rid);			//inserting data into current leaf page as there is space available
				unpinPage(currentLeafPage.getCurPage(), true);	//unpin the leaf page after the record is inserted
				return null;
			}
			else 		//leaf page does not have space for entries
			{
				BTLeafPage newLeafPage = new BTLeafPage(headerPage.get_keyType());	//create a new leaf page
				PageId newLeafPageID = newLeafPage.getCurPage();	//get the page id of the newly created leaf page
				newLeafPage.setNextPage(currentLeafPage.getNextPage());  //Next page of new leaf points to the next page of old leaf
				currentLeafPage.setNextPage(newLeafPageID);             //next page of old leaf points to new leaf
				newLeafPage.setPrevPage(currentLeafPage.getCurPage());  //Previous page of new leaf points to old leaf
				KeyDataEntry temp = null, lastTemp = null;
				RID delRid = new RID();
				System.out.println(currentLeafPage.getFirst(delRid).data);
				int count=0;	//a variable is created to count the number of records in the leaf page
				for(temp = currentLeafPage.getFirst(delRid); temp!=null; temp = currentLeafPage.getNext(delRid))
				{
					count++;	//increment the counter
				}
				System.out.println("No of records in old leaf = " + count);
				temp = currentLeafPage.getFirst(delRid); //Get the first entry into the old leaf
				for(int i=1;i<=count;i++)
				{
					if(i>count/2)	// dividing the number of records into two halves
					{
						LeafData lfData = (LeafData)temp.data;
						System.out.println(lfData);
						newLeafPage.insertRecord(temp.key, lfData.getData()); // Insert data into the split page
						currentLeafPage.deleteSortedRecord(delRid);  //Delete the moved record from old leaf page
						temp = currentLeafPage.getCurrent(delRid); 

					}
					else
					{
						lastTemp = temp;
						temp = currentLeafPage.getNext(delRid);
					}
				}
				if(BT.keyCompare(key, lastTemp.key)>0)
				{
					newLeafPage.insertRecord(key,rid);
				}
				else
				{
					currentLeafPage.insertRecord(key, rid);
				}
				
				unpinPage(currentLeafPage.getCurPage(), true);		//unpin the current page
				
				KeyDataEntry copy;   
				
				temp = newLeafPage.getFirst(delRid);   
				
				copy = new KeyDataEntry(temp.key, newLeafPageID); 	//The first entry record of the new split leaf node is copied to index node
				
				
				unpinPage(newLeafPageID, true);
				
				return copy;
	
			}
		}
		else if(currentPage.getType() == NodeType.INDEX)		//if current page is a index type page
		{
			BTIndexPage currentIndexPage = new BTIndexPage(currentPageId, headerPage.get_keyType());	//create an instance of an index page
			PageId currentIndexPageId = currentIndexPage.getPageNoByKey(key);		// get the page id of the created index page
			unpinPage(currentIndexPage.getCurPage());		//unpin the current index page
			KeyDataEntry upEntry = null;
			upEntry = _insert(key, rid, currentIndexPageId);
			if(upEntry==null)	//split has not occured
			{
				return null;
			}
			else
			{
				if(currentIndexPage.available_space()>BT.getKeyDataLength(upEntry.key, NodeType.INDEX))	//index page has space for entries
				{
					
					IndexData indexRecord = (IndexData) upEntry.data;
					currentIndexPage.insertKey(upEntry.key, indexRecord.getData());		//inserting data into current index page
					unpinPage(currentIndexPage.getCurPage(), true);
				}
				else		//index page does not have space for the record to be inserted
				{
					BTIndexPage newIndexPage = new BTIndexPage(headerPage.get_keyType());	//create a new index page
					KeyDataEntry temp = null, lastTemp = null;
					RID delRid = new RID();
					for(temp = currentIndexPage.getFirst(delRid); temp!=null; temp = currentIndexPage.getFirst(delRid))	//transfer records from current index page to new index page
					{
					
						System.out.println(temp.key);
						IndexData indexRecord = (IndexData)temp.data;
						newIndexPage.insertKey(temp.key, indexRecord.getData());	//insert records on new index page
						
						currentIndexPage.deleteSortedRecord(delRid);	//delete the records from current index page
					}
					
					for(temp = newIndexPage.getFirst(delRid); newIndexPage.available_space()< currentIndexPage.available_space();
							temp = newIndexPage.getFirst(delRid))	//split the records
					{
						
										
						IndexData indexRecord = (IndexData)(temp.data);	
						currentIndexPage.insertKey(temp.key, indexRecord.getData());
						
						newIndexPage.deleteSortedRecord(delRid);
						lastTemp = temp;
					
					}
					
					temp = newIndexPage.getFirst(delRid);
					
					
					if(BT.keyCompare(upEntry.key, temp.key) > 0)		
					{
						IndexData indexRecord = (IndexData)(upEntry.data);	
						newIndexPage.insertKey(upEntry.key, indexRecord.getData());		//new key moves to new index page
					}
					else
					{
						IndexData indexRecord = (IndexData)(upEntry.data);	
						currentIndexPage.insertKey(upEntry.key, indexRecord.getData());	//new key will be on the current index page only
						
					}
					unpinPage(currentIndexPage.getCurPage(), true);		//unpin the current index page
					upEntry = newIndexPage.getFirst(delRid);
					
					
					
					newIndexPage.setPrevPage(((IndexData)upEntry.data).getData());		//set the left link of the new index page to the node where the data is referring to
					
					newIndexPage.deleteSortedRecord(delRid); //Delete the first record from new index page
										
					unpinPage(newIndexPage.getCurPage(), true);			//unpin the new index page
					
					
					((IndexData)upEntry.data).setData(newIndexPage.getCurPage());	//set the higher index page to the index new page
					
				
					return upEntry;
				}
			}
			
		}
		else
		{
			throw new InsertException(null,"");
		}
		
		return null;
	}

	



	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 *
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid) throws DeleteFashionException,
			LeafRedistributeException, RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException,
			IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException,
			IOException {
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			return NaiveDelete(key, rid);
		else
			throw new DeleteFashionException(null, "");
	}

	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key
	 * is null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text
	 * (for the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way
	 * left if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException,
			IteratorException, KeyNotMatchException, ConstructPageException,
			PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null
					&& BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno),
						headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necesary, to
	 * find the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException {
	
		 BTLeafPage leafPage;			//creating a leaf page
		 RID iteratorID = new RID();	//iterator of type RID
		 KeyDataEntry entry;
		 leafPage = findRunStart(key, iteratorID);		//finding the first page and RID of the key
		 if (leafPage == null)
				return false;
		 entry = leafPage.getCurrent(iteratorID);
		 RID firstRID = new RID();		//first record  of the leaf page
			int del=0; 	
			while(true)
			{
				while (entry == null)
				{
					PageId nextpage = leafPage.getNextPage();		//traverse to the next page
					unpinPage(leafPage.getCurPage());				//unpin the previous page after traversing to the next page
					if (nextpage.pid == INVALID_PAGE)
						return false;
					Page nextPage = pinPage(nextpage);
					leafPage = new BTLeafPage(nextPage, headerPage.get_keyType());		//initialize leaf page to the net page
					entry = leafPage.getFirst(firstRID);			//initialize entry by getting the first record ID of the leaf page
					
				}
				if (BT.keyCompare(key, entry.key) > 0) 		//Checking the key and the entry key
					break;
				while(leafPage.delEntry(new KeyDataEntry(key, rid)) == true)		//key is found and it is deleted
				{
					entry = leafPage.getCurrent(iteratorID);	//move to the next page
					del=1;		//key is deleted
				}
				if(entry == null)  
				{   
					continue;		//traverse through all the leaf pages
				}
				else                     //If next entry is not key value then stop the deletion and exit
				{
					break;
				}
			}
			unpinPage(leafPage.getCurPage());		//unpin the leaf page
			if(del==1)
			{
				return true; 		//if records are deleted
			}else
			return false; 			//If no records were deleted
}

			
	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index (2) lo_key = null, hi_key!= null range scan from min
	 * to the hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key
	 * to max (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (
	 * might not unique) (5) lo_key!= null, hi_key!= null, lo_key < hi_key range
	 * scan from lo_key to hi_key
	 *
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws IOException, KeyNotMatchException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id) throws IOException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
