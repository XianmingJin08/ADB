#include <string.h>
#include "btindex.h"


//-------------------------------------------------------------------
// BTIndexPage::Insert
//
// Input   : key - value of the key to be inserted.
//           pageID - page id associated to that key.
// Output  : rid - record id of the (key, pageID) record inserted.
// Purpose : Insert the pair (key, pageID) into this index node.
// Return  : OK if insertion is succesfull, FAIL otherwise.
//-------------------------------------------------------------------

Status 
BTIndexPage::Insert(const int key, const PageID pageID, RecordID& rid)
{
	IndexEntry entry;
	entry.key = key;
	entry.pid = pageID;

	Status s = SortedPage::InsertRecord((char *)&entry, sizeof(IndexEntry), rid);
	if (s != OK)
	{
		cerr << "Fail to insert record into IndexPage" << endl;
		return FAIL;
	}
	
	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::Delete
//
// Input   : key  - value of the key to be deleted.
// Output  : rid - record id of the (key, pageID) record deleted.
// Purpose : Delete the entry associated with key from this index node.
// Return  : OK if deletion is succesfull, FAIL otherwise. If FAIL is
//           returned, rid may contain garbage.
//-------------------------------------------------------------------

Status 
BTIndexPage::Delete (const int key, RecordID& rid)
{
	// Scan through all the records in this page and find the
	// matching pair (key, dataRid).

	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		IndexEntry* entry = (IndexEntry *)(data + slots[i].offset);
		if (entry->key == key)
		{
			// We delete it here.
			rid.pageNo = PageNo();
			rid.slotNo = i;
			Status s = SortedPage::DeleteRecord(rid);
			return s;
		}
	}
	
	return FAIL;
}


//-------------------------------------------------------------------
// BTIndexPage::GetFirst
//
// Input   : None
// Output  : rid - record id of the first entry
//           firstKey - pointer to the key value
//           firstPid - the page id
// Purpose : get the first pair (firstKey, firstPid) in the index page 
//           and it's rid.
// Return  : OK if a record is returned,  DONE if no record exists on 
//           this page.
//-------------------------------------------------------------------

Status 
BTIndexPage::GetFirst(int& firstKey, PageID& firstPid, RecordID& rid)
{
	// Initialize the record id of the first (key, dataRid) pair.  The
	// first record is always at slot position 0, since SortedPage always
	// compact it's records.  We can also use HeapPage::FirstRecord here
	// but it is not neccessary.

	rid.pageNo = pid;
	rid.slotNo = 0;

	// If there are no record in this page, just return DONE.
	
	if (numOfSlots == 0)
	{
		rid.pageNo = INVALID_PAGE;
		rid.slotNo = INVALID_SLOT;
		return DONE;
	}
	
	// Otherwise, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset slots[0].offset
	// from the beginning of data area (pointed to by member data in 
	// HeapPage)

	IndexEntry entry;
	memcpy(&entry, (IndexEntry *)(data + slots[0].offset), sizeof(IndexEntry));
	firstKey = entry.key;
	firstPid = entry.pid;
	
	return OK;
}


//-------------------------------------------------------------------
// BTIndexPage::GetNext
//
// Input   : rid - record id of the current entry
// Output  : rid - record id of the next entry
//           nextKey - the key value of next entry
//           nextPid - the page id of next entry
// Purpose : Get the next pair (nextKey, nextPid) in the index page and 
//           it's rid.
// Return  : OK if there is a next record, DONE if no more.  If DONE is
//           returned, rid is set to invalid.
//-------------------------------------------------------------------

Status 
BTIndexPage::GetNext(int& nextKey, PageID& nextPid, RecordID& rid)
{
	// If we are at the end of records, return DONE.

	if (rid.slotNo + 1 >= numOfSlots)
	{
		rid.pageNo = INVALID_PAGE;
		rid.slotNo = INVALID_SLOT;
		return DONE;
	}

	// Increment the slotNo in rid to point to the next record in this
	// page.  We can do this for subclass of SorterPage since the records
	// in a sorted page is always compact.  
	
	rid.slotNo++;

	// Otherwise, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset 
	// slots[rid.slotNo].offset from the beginning of data area 
	// (pointed to by member data in HeapPage)

	IndexEntry entry;
	memcpy(&entry, (IndexEntry *)(data + slots[rid.slotNo].offset), sizeof(IndexEntry));
	nextKey = entry.key;
	nextPid = entry.pid;
	
	return OK;
}
//-------------------------------------------------------------------
// BTIndexPage::FindPageWithKey
//
// Input   : d_key - the key we use to compare
// Output  : rid - record id of the the entry
//           key - the key value of the entry
//           pid - the page id of the entry
// Purpose : Get the the pair (key, pid) in the index page and 
//           it's rid for given d_key.
// Return  : OK if found pid, FAIL otherwise
//-------------------------------------------------------------------

Status 
BTIndexPage::FindPageWithKey(const int d_key, int& key, PageID& pid, RecordID& rid)
{
	Status s = GetFirst(key,pid,rid);
	if (s != OK){
		return FAIL;
	}
	if (d_key<key){
		pid = GetLeftLink();
		return OK;
	}

	while(s != DONE){
		PageID prev;
	 	int key_prev;
	 	RecordID rid_prev;
		if (d_key<key){
			key = key_prev;
			pid = prev;
			rid = rid_prev;
			return OK;
		}
		key_prev = key;
		prev = pid;
		rid_prev = rid;
		s = GetNext(key,pid,rid);

	}
	return OK;
}
//-------------------------------------------------------------------
// BTIndexPage::FindPageWithKeys
//
// Input   : d_key - the key we use to compare
// Output  : rid - record id of the the entry
//           key - the key value of the entry
//           pid - the page id of the entry
// Purpose : Get the the pair (key, pid) in the index page and 
//           it's rid for given d_key.
// Return  : OK if found pid, FAIL otherwise
//-------------------------------------------------------------------

Status 
BTIndexPage::FindPageWithKeys(const int d_key, int& key,int& nextKey, PageID& pid, PageID& prevPid, PageID& nextPid, RecordID& rid)
{	
	PageID pid_tmp;
	int key_tmp;
	RecordID rid_tmp;
	Status s = GetFirst(key_tmp,pid_tmp,rid_tmp);
	if (s != OK){
		return FAIL;
	}
	if (d_key < key_tmp){
		prevPid = INVALID_PAGE;
		pid = GetLeftLink();
		nextPid = pid_tmp;
		nextKey = key_tmp;
		key = -1;
		return OK;
	}
	PageID prev_prev_pid = INVALID_PAGE;
	PageID prev_pid = GetLeftLink();
	int prev_key;
	while (s != DONE){
		if (d_key < key_tmp){
			prevPid = prev_prev_pid;
			pid = prev_pid;
			key = prev_key;
			nextKey = key_tmp;
			nextPid = pid_tmp;
			return OK;
		}
		prev_prev_pid = prev_pid;
		prev_pid = pid_tmp;
		prev_key = key_tmp;
		s = GetNext(key_tmp,pid_tmp,rid_tmp);

	}
	prevPid = prev_prev_pid;
	pid = prev_pid;
	key = prev_key;
	nextKey = key_tmp;
	nextPid = pid_tmp;
	return OK;
}

//-------------------------------------------------------------------
//BTLeafPage::GetLast
//-------------------------------------------------------------------
Status
BTIndexPage::GetLast(int& key, PageID& pid, RecordID& rid)
{
	Status s = GetFirst(key,pid,rid);
	while (s != DONE){
		s = GetNext(key,pid,rid);
	}
	return OK;
}
//-------------------------------------------------------------------
// BTIndexPage::GetLeftLink
//
// Input   : None
// Output  : None
// Purpose : Return the page id of the page at the left of this page.
// Return  : The page id of the page at the left of this page.
//-------------------------------------------------------------------

PageID BTIndexPage::GetLeftLink()
{
	return GetPrevPage();
}


//-------------------------------------------------------------------
// BTIndexPage::SetLeftLink
//
// Input   : pageID - new left link
// Output  : None
// Purpose : Set the page id of the page at the left of this page.
// Return  : None
//-------------------------------------------------------------------

void BTIndexPage::SetLeftLink(PageID pageID)
{
	SetPrevPage(pageID);
}


