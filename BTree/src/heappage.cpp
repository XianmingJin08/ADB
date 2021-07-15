#include <iostream>
#include <stdlib.h>
#include <memory.h>

#include "heappage.h"
#include "bufmgr.h"
#include "db.h"


//------------------------------------------------------------------
// Constructor of HeapPage
//
// Input     : Page ID
// Output    : None
//------------------------------------------------------------------

void HeapPage::Init(PageID pageNo)
{
	pid = pageNo;
	prevPage = INVALID_PAGE;
	nextPage = INVALID_PAGE;
	fillPtr = sizeof(data); 					// fill data from the end of the page
	freeSpace = sizeof(data) + sizeof(Slot); 	// add sizeof(Slot) as numOfSlots == 0
	numOfSlots = 0;
	SLOT_SET_EMPTY(slots[0]);
}

void HeapPage::SetNextPage(PageID pageNo)
{
	nextPage = pageNo;
}

void HeapPage::SetPrevPage(PageID pageNo)
{
	prevPage = pageNo;
}

PageID HeapPage::GetNextPage()
{
	return nextPage;
}

PageID HeapPage::GetPrevPage()
{
	return prevPage;
}


//------------------------------------------------------------------
// HeapPage::InsertRecord
//
// Input     : Pointer to the record and the record's length 
// Output    : Record ID of the record inserted.
// Purpose   : Insert a record into the page
// Return    : OK if everything went OK, DONE if sufficient space 
//             does not exist
//------------------------------------------------------------------

Status HeapPage::InsertRecord(char *recPtr, int length, RecordID& rid)
{
	// check if there is enough space.
	if (AvailableSpace() < length)
	{
		return DONE; // no enough space
	}

	// OK, we have enough space.  Prepare for insertion

	fillPtr -= length; 		// allocate the space for the record
	freeSpace -= length;

	// Now look for the first empty slot by scanning the slot directory.

	int sid;
	for (sid = 0; sid < numOfSlots; sid++)
	{
		if (SLOT_IS_EMPTY(slots[sid]))
		{
			// Found an empty slot. Use it.
			break;
		}
	}

	if (sid == numOfSlots)
	{
		// No more empty slots. Create new one.
		numOfSlots++;
		freeSpace -= sizeof(Slot);
	}

	SLOT_FILL(slots[sid], fillPtr, length);

	// Next, copy the record into the data area.
	memcpy(&data[fillPtr], recPtr, length);

	// Output the record id
	rid.pageNo = pid;
	rid.slotNo = sid;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::DeleteRecord 
//
// Input    : Record ID
// Output   : None
// Purpose  : Delete a record from the page
// Return   : OK if successful, FAIL otherwise  
//------------------------------------------------------------------ 

Status HeapPage::DeleteRecord(const RecordID& rid)
{
	// Check validity of rid

	if (rid.pageNo != this->pid)
	{
		cerr << "Invalid Page No " << rid.pageNo << endl;
		return FAIL;
	}

	if (rid.slotNo >= numOfSlots || rid.slotNo < 0)
	{
		cerr << "Invalid Slot No " << rid.slotNo << endl;
		return FAIL;
	}

	if (SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		cerr << "Slot " << rid.slotNo << " is empty." << endl;
		return FAIL;
	}

	// ASSERT : rid is valid.

	short offset = slots[rid.slotNo].offset;
	short length = slots[rid.slotNo].length;

	if (rid.slotNo == numOfSlots - 1)
	{
		// If we delete the record which occupy the last slot,
		// we can remove any empty slots at the end of the
		// slots directory.
		numOfSlots--;
		freeSpace += sizeof(Slot);

		while (numOfSlots > 0 && SLOT_IS_EMPTY(slots[numOfSlots - 1]))
		{
			numOfSlots--;
			freeSpace += sizeof(Slot);
		}
	}
	else
	{
		SLOT_SET_EMPTY(slots[rid.slotNo]);
	}

	if (fillPtr < offset) {
		// Now move all shift all records.  Expensive but we assume
		// deletion is rare.
		memmove( &data[fillPtr + length], &data[fillPtr], offset - fillPtr );

		// Update the slots directory.
		for (int i = 0; i < numOfSlots; i++)
		{
			if (!SLOT_IS_EMPTY(slots[i]) && slots[i].offset < offset)
			{
				slots[i].offset += length;
			}
		}
	}

	freeSpace += length;
	fillPtr += length;

	return OK;
}


//------------------------------------------------------------------
// HeapPage::FirstRecord
//
// Input    : None
// Output   : record id of the first record on a page
// Purpose  : To find the first record on a page
// Return   : OK if successful, DONE otherwise
//------------------------------------------------------------------

Status HeapPage::FirstRecord(RecordID& rid)
{
	for (int i = 0; i < numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY(slots[i]))
		{
			rid.slotNo = i;
			rid.pageNo = pid;
			return OK;
		}
	}

	return DONE;
}


//------------------------------------------------------------------
// HeapPage::NextRecord
//
// Input    : ID of the current record
// Output   : ID of the next record
// Return   : Return DONE if no more records exist on the page; 
//            otherwise OK. In case of an error, return FAIL.
//------------------------------------------------------------------

Status HeapPage::NextRecord(RecordID curRid, RecordID& nextRid)
{
	// Check validity of rid

	if (curRid.pageNo != this->pid)
	{
		cerr << "Invalid Page No " << curRid.pageNo << endl;
		return FAIL;
	}

	if (curRid.slotNo >= numOfSlots || curRid.slotNo < 0)
	{
		cerr << "Invalid Slot No " << curRid.slotNo << endl;
		return FAIL;
	}

	// scanning
	for (int i = curRid.slotNo + 1; i < numOfSlots; i++)
	{
		if (!SLOT_IS_EMPTY(slots[i]))
		{
			nextRid.slotNo = i;
			nextRid.pageNo = pid;
			return OK;
		}
	}

	return DONE;
}


//------------------------------------------------------------------
// HeapPage::GetRecord
//
// Input    : Record ID
// Output   : Records length and a copy of the record itself
// Purpose  : To retrieve a _copy_ of a record with ID rid from a page
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::GetRecord(RecordID rid, char *recPtr, int& length)
{
	// Check validity of rid

	if (rid.pageNo != this->pid)
	{
		cerr << "Invalid Page No " << rid.pageNo << endl;
		return FAIL;
	}

	if (rid.slotNo >= numOfSlots || rid.slotNo < 0)
	{
		cerr << "Invalid Slot No " << rid.slotNo << endl;
		return FAIL;
	}

	if (SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		cerr << "Slot " << rid.slotNo << " is empty." << endl;
		return FAIL;
	}

	length = slots[rid.slotNo].length;
	memcpy(recPtr, &data[slots[rid.slotNo].offset], length);

	return OK;
}


//------------------------------------------------------------------
// HeapPage::ReturnRecord
//
// Input    : Record ID
// Output   : pointer to the record, record's length
// Purpose  : To output a _pointer_ to the record
// Return   : OK if successful, FAIL otherwise
//------------------------------------------------------------------

Status HeapPage::ReturnRecord(RecordID rid, char*& recPtr, int& length)
{
	// Check validity of rid

	if (rid.pageNo != this->pid)
	{
		cerr << "Invalid Page No " << rid.pageNo << endl;
		return FAIL;
	}

	if (rid.slotNo >= numOfSlots || rid.slotNo < 0)
	{
		cerr << "Invalid Slot No " << rid.slotNo << endl;
		return FAIL;
	}

	if (SLOT_IS_EMPTY(slots[rid.slotNo]))
	{
		cerr << "Slot " << rid.slotNo << " is empty." << endl;
		return FAIL;
	}

	length = slots[rid.slotNo].length;
	recPtr = &data[slots[rid.slotNo].offset];

	return OK;
}


//------------------------------------------------------------------
// HeapPage::AvailableSpace
//
// Input    : None
// Output   : None
// Purpose  : To return the amount of available space
// Return   : The amount of available space on the heap file page.
//------------------------------------------------------------------

int HeapPage::AvailableSpace(void)
{
	for (int i = 0; i < numOfSlots; i++)
	{
		if (SLOT_IS_EMPTY(slots[i]))
			return freeSpace;
	}

	// preserve a place for the slot of the next inserted record
	return freeSpace - sizeof(Slot);
}


//------------------------------------------------------------------
// HeapPage::IsEmpty
// 
// Input    : None
// Output   : None
// Purpose  : Check if there is any record in the page.
// Return   : true if the HeapPage is empty, and false otherwise.
//------------------------------------------------------------------

bool HeapPage::IsEmpty(void)
{
	// Since we shrink the slot directory when we delete,
	// an empty page must have numOfSlots == 0.
	return (numOfSlots == 0);
}


//------------------------------------------------------------------
// HeapPage::GetNumOfRecords
// 
// Input    : None
// Output   : None
// Purpose  : To return the number of records in the page.
// Return   : The number of records currently stored in the page.
//------------------------------------------------------------------

int HeapPage::GetNumOfRecords()
{
	int sum = 0;
	for (int i = 0; i < numOfSlots; i++)
	{
		if (!(SLOT_IS_EMPTY(slots[i])))
		{
			sum++;
		}
	}
	return sum;
}


//------------------------------------------------------------------
// HeapPage::CompactSlotDir
// 
// Input    : None
// Output   : None
// Purpose  : To compact the slot directory (eliminate empty slots).
// Return   : None
//------------------------------------------------------------------

void HeapPage::CompactSlotDir()
{
	int j = 0;
	for (int i = 0; i < numOfSlots; i++) {
		if (!SLOT_IS_EMPTY(slots[i])) {
			if (j < i) {
				SLOT_FILL(slots[j], slots[i].offset, slots[i].length);
				SLOT_SET_EMPTY(slots[i]);
			}
			j++;
		}
	}

	freeSpace += sizeof(Slot) * (numOfSlots-j);
	numOfSlots = j;
}
