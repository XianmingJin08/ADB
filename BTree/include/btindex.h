#ifndef BTINDEX_PAGE_H
#define BTINDEX_PAGE_H


#include "minirel.h"
#include "page.h"
#include "sortedpage.h"
#include "bt.h"



class BTIndexPage : public SortedPage {
	
private:

	// No private or public members should be declared.

public:
	
	// You may add public methods here.
	
	Status Insert(const int key, const PageID pid, RecordID& rid);
	Status Delete(const int key, RecordID& rid);

	Status GetFirst(int& key, PageID& pid, RecordID& rid);
	Status GetNext(int& key, PageID& pid, RecordID& rid);
	PageID GetLeftLink(void);
	void SetLeftLink(PageID left);
	Status FindPageWithKey(const int d_key,int& key, PageID& pid, RecordID& rid);   
	Status FindPageWithKeys(const int d_key, int& key,int& nextKey, PageID& pid, PageID& prevPid, PageID& nextPid, RecordID& rid);
	Status GetLast(int& key, PageID& pid, RecordID& rid);

	IndexEntry* GetEntry(int slotNo) 
	{
		return (IndexEntry *)(data + slots[slotNo].offset);
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
		PageID pid_tmp;
		RecordID rid_tmp;
		GetFirst(key_tmp,pid_tmp,rid_tmp);
		Delete(key_tmp,rid_tmp);
		if (! IsAtLeastHalfFull ()){
			return false;
		}
		Insert(key_tmp,pid_tmp,rid_tmp);
		return true;
	}
};

#endif
