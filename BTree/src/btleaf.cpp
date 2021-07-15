#include <memory.h>
#include "btleaf.h"


//-------------------------------------------------------------------
// BTLeafPage::Insert
//
// Input   : key  - value of the key to be inserted.
//           dataRid - record id of the record associated with key.
// Output  : pairRid - record id of the inserted pair (key, dataRid)
// Purpose : Insert the pair (key, dataRid) into this leaf node.
// Return  : OK if insertion is successful.  FAIL otherwise.
//-------------------------------------------------------------------

Status 
BTLeafPage::Insert(const int key, const RecordID dataRid, RecordID& pairRid)
{
	LeafEntry entry;	
	entry.key = key;
	entry.rid = dataRid;
	
	Status s = SortedPage::InsertRecord((char *)&entry, sizeof(LeafEntry), pairRid);
	if (s != OK)
	{
		cerr << "Fail to insert record into LeafPage" << endl;
		return FAIL;
	}
	
	return OK;
}

//-------------------------------------------------------------------
// BTLeafPage::Delete
//
// Input   : key  - value of the key to be deleted
//			 dataRid - record id of the record associated with key.
// Output  : rid - record id of the deleted record.
// Purpose : Find the pair (key, dataRid) and delete it.
// Return  : OK if successful, FAIL otherwise.  If FAIL is returned, rid's
//           content may be garbage.
//-------------------------------------------------------------------

Status 
BTLeafPage::Delete(const int key, const RecordID dataRid, RecordID& rid)
{
	// Scan through all the records in this page and find the
	// matching pair (key, dataRid).

	for (int i = numOfSlots - 1; i >= 0; i--)
	{
		LeafEntry* entry = GetEntry(i); 
		if (entry->rid == dataRid && entry->key == key)
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
// BTLeafPage::GetFirst
//
// Input   : None
// Output  : rid - record id of the first entry
//           key - the key value
//           dataRid - the record id of the record associated with key
// Purpose : get the first pair (key, dataRid) in the leaf page and 
//           it's rid.
// Return  : OK if the first record is returned. If there are no more
//           record in this page, DONE is returned and dataRid is set
//           to invlid.
//-------------------------------------------------------------------


Status 
BTLeafPage::GetFirst(int& key, RecordID& dataRid, RecordID& rid)
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
		dataRid.pageNo = INVALID_PAGE;
		dataRid.slotNo = INVALID_SLOT;
		return DONE;
	}
	
	// Otherwise, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset slots[0].offset
	// from the beginning of data area (pointed to by member data in 
	// HeapPage)

	LeafEntry entry;
	memcpy(&entry, GetEntry(0), sizeof(LeafEntry));
	key = entry.key;
	dataRid = entry.rid;
	
	return OK;
}


//-------------------------------------------------------------------
// BTLeafPage::GetNext
//
// Input   : rid - record id of the current entry
// Output  : rid - record id of the next entry
//           key - pointer to the key value
//           dataRid - the record id of the record associated with key
// Purpose : get the next pair (key, dataRid) in the leaf page and it's
//           rid.
// Return  : OK if there is a next record, DONE if no more.  If DONE is
//           returned, then rid is unchanged and dataRid is set to invalid.
//-------------------------------------------------------------------


Status 
BTLeafPage::GetNext(int& key, RecordID& dataRid, RecordID& rid)
{
	// If we are at the end of records, return DONE.

	if (rid.slotNo + 1 >= numOfSlots)
	{
		dataRid.pageNo = INVALID_PAGE;
		dataRid.slotNo = INVALID_SLOT;
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

	LeafEntry entry;
	memcpy(&entry, GetEntry(rid.slotNo), sizeof(LeafEntry));
	key = entry.key;
	dataRid = entry.rid;
	
	return OK;
}

//-------------------------------------------------------------------
// BTLeafPage::FindAndRemove
//
// Input   : key  - value of the key to be deleted
//			 dataRid - record id of the record associated with key.
// Output  : None.
// Purpose : Find the pair (key, dataRid) and delete it.
// Return  : OK if successful, FAIL otherwise. 
//-------------------------------------------------------------------
Status 
BTLeafPage::FindAndRemove (int key, RecordID dataRid)
{
	int key_tmp;
	RecordID dataRid_tmp,rid_tmp;
	Status s = GetFirst(key_tmp,dataRid_tmp,rid_tmp);
	while (s != DONE){
		if (key_tmp == key && dataRid_tmp == dataRid){
			Delete(key,dataRid,rid_tmp);
			return OK;
		}
		s = GetNext(key_tmp,dataRid_tmp,rid_tmp);
	}
	return FAIL;	
}
//-------------------------------------------------------------------
//BTLeafPage::GetLast
//-------------------------------------------------------------------
Status
BTLeafPage::GetLast(int& key, RecordID& dataRid, RecordID& rid)
{
	Status s = GetFirst(key,dataRid,rid);
	if (s != OK){return FAIL;}
	while (s != DONE){
		s = GetNext(key,dataRid,rid);
	}
	return OK;
}
//-------------------------------------------------------------------
// BTLeafPage::GetCurrent
//
// Input   : rid - record id of the current entry
// Output  : key - pointer to the key value
//           dataRid - the record id of the record associated with key
// Purpose : get the current pair (key, dataRid) in the leaf page and it's
//           rid.
// Return  : OK if a record is returned.  Return DONE if record with
//           rid does not exists.  If DONE is returned, dataRID is 
//           set to invalid.
//-------------------------------------------------------------------

Status 
BTLeafPage::GetCurrent(int& key, RecordID& dataRid, RecordID rid)
{
	// Check if the current record id is valid.  If not, return
	// DONE.

	if (rid.slotNo >= numOfSlots)
	{
		dataRid.pageNo = INVALID_PAGE;
		dataRid.slotNo = INVALID_SLOT;
		return DONE;
	}

	// If it's valid, we just copy the record into key and dataRid,
	// and returned.  The record is located at offset 
	// slots[rid.slotNo].offset from the beginning of data area 
	// (pointed to by member data in HeapPage)

	LeafEntry entry;
	memcpy(&entry, GetEntry(rid.slotNo), sizeof(LeafEntry));
	key = entry.key;
	dataRid = entry.rid;
	
	return OK;
}

