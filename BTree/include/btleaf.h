#ifndef BTLEAF_PAGE_H
#define BTLEAF_PAGE_H

#include "minirel.h"
#include "page.h"
#include "sortedpage.h"
#include "bt.h"
#include "btindex.h"


class BTLeafPage : public SortedPage {
	
private:
	
	// No private or public members should be declared.
	
public:
		
	Status Insert(const int key, const RecordID dataRid, RecordID& rid);
	Status Delete(const int key, const RecordID dataRid, RecordID& rid);
	
	Status GetFirst(int& key, RecordID& dataRid, RecordID& rid);
	Status GetNext(int& key, RecordID& dataRid, RecordID& rid);
	Status GetCurrent(int& key, RecordID& dataRid, RecordID rid);
	Status GetLast(int& key, RecordID& dataRid, RecordID& rid);
	Status FindAndRemove (int key, RecordID dataRid);
	LeafEntry* GetEntry(int slotNo)
	{
		return (LeafEntry *)(data + slots[slotNo].offset);
	}

	bool IsAtLeastHalfFull()
	{
		return (AvailableSpace() <= (HEAPPAGE_DATA_SIZE) / 2);
	}
	bool IsAtLeastHalfFullAfterDelete(){
		if (! IsAtLeastHalfFull() ){
			return false;
		}
		int key_tmp; 
		RecordID dataRid_tmp,rid_tmp;
		GetFirst(key_tmp,dataRid_tmp,rid_tmp);
		Delete(key_tmp,dataRid_tmp,rid_tmp);
		if (! IsAtLeastHalfFull ()){
			return false;
		}
		Insert(key_tmp,dataRid_tmp,rid_tmp);
		return true;
	}
};

#endif
