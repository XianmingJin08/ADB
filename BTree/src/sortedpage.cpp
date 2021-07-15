/*
* sorted_page.cpp - implementation of class SortedPage
*
*/

#include "sortedpage.h"
#include "btindex.h"
#include "btleaf.h"

//-------------------------------------------------------------------
// SortedPage::InsertRecord
//
// Input   : recPtr  - pointer to the record to be inserted
//           recLen  - length of the record
// Output  : rid - record id of the inserted record
// Precond : There is enough space on this page to accomodate this
//           record.  The records on this page is sorted and the
//           slots directory is compact.
// Postcond: The records on this page is still sorted and the
//           slots directory is compact.
// Purpose : Insert the record into this page.
// Return  : OK if insertion is done, FAIL otherwise.
//-------------------------------------------------------------------

Status SortedPage::InsertRecord(char * recPtr, int recLen, RecordID& rid)
{
	// ASSERTIONS:
	// - the slot directory is compressed -> inserts will occur at the end
	// - slotCnt gives the number of slots used
	
	// general plan:
	//    1. Insert the record into the page,
	//       which is then not necessarily any more sorted
	//    2. Sort the page by rearranging the slots (insertion sort)
	
	Status status = HeapPage::InsertRecord(recPtr, recLen, rid);
	if (status != OK) 
	{
		return FAIL;
	}
	
	// performs a simple insertion sort
	int i;
	for (i = numOfSlots - 1; i > 0; i--) 
	{
		int *x = (int *)(data + slots[i].offset);
		int *y = (int *)(data + slots[i - 1].offset);
		
		if (*x < *y)
		{
			// switch slots
			Slot tmpSlot;
			tmpSlot = slots[i];
			slots[i] = slots[i - 1];
			slots[i - 1] = tmpSlot;
		}
		else
		{
			// end insertion sort
			break;
		}
	}
	
	// ASSERTIONS:
	// - record keys increase with increasing slot number (starting at slot 0)
	// - slot directory compacted
	
	rid.slotNo = i;
		
	return OK;
}


//-------------------------------------------------------------------
// SortedPage::DeleteRecord
//
// Input   : rid - record id of the record to be deleted.
// Output  : None
// Postcond: The slots directory is compact.
// Purpose : Delete a record from this page, and compact the slot
//           directory.
// Return  : OK is deletion is successfull.  FAIL otherwise.
//-------------------------------------------------------------------

Status SortedPage::DeleteRecord(const RecordID& rid)
{
	Status status = HeapPage::DeleteRecord(rid);	
	if (status != OK) 
	{
		return FAIL;
	}

	HeapPage::CompactSlotDir();
	
	// ASSERTIONS:
	// - slot directory is compacted

	return OK;
}

