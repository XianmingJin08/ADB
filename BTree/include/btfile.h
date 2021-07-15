#ifndef _BTFILE_H
#define _BTFILE_H

#include "btindex.h"
#include "btleaf.h"
#include "index.h"
#include "btfilescan.h"
#include "bt.h"

class BTreeFile: public IndexFile {

public:

	friend class BTreeFileScan;

	BTreeFile(Status& status, const char* filename);
	~BTreeFile();

	Status DestroyFile();

	Status Insert(const int key, const RecordID rid);
	Status Delete(const int key, const RecordID rid);

	IndexFileScan* OpenScan(const int* lowKey, const int* highKey);

	Status Print();
	Status DumpStatistics();

private:

	// You may add members and methods here.

	PageID rootPid;
	const char* fname;

	void setRootPid(PageID pid) { rootPid = pid; }
	void setFileName(const char* filename){fname=filename;}
	Status DestroyFileHelper(PageID curPid);
	Status InsertHelper (const int key, const RecordID rid, PageID curPid, bool& split, int& child_key, PageID& child_pageid);
	Status Split_Leaf(BTLeafPage* oldPage, BTLeafPage* newPage, const int key,const RecordID rid);
	Status Split_Index(BTIndexPage* oldPage, BTIndexPage* newPage, const int key, PageID pid, int& newPageKey);
	Status DeleteHelper(const int key, const RecordID rid, const int curKey,const int nextKey,PageID curPid, PageID prevPid,PageID nextPid, bool& underflow, bool& merge,int& child_key, PageID& child_pageid, int& deletedKey);
	Status DeleteLeaf_prev(BTLeafPage* prevPage, BTLeafPage* curPage, int& newKey);
	Status DeleteLeaf_next(BTLeafPage* nextPage, BTLeafPage* curPage, int& newKey);
	Status MergeLeaf_prev(BTLeafPage* prevPage, BTLeafPage* curPage);
	Status MergeLeaf_next(BTLeafPage* nextPage, BTLeafPage* curPage);
	PageID GetMinimumPid(int & key,int & height );
	PageID GetMaxKey(int & key);
	PageID FindPidWithKey(const int key);
	Status PrintTree(PageID pid);
	Status PrintNode(PageID pid);
	Status Sum_Index_nodes(PageID curpid, int& nodes, int& num_of_records, float& sum_fill, float& max_fill,float& min_fill);
	Status Sum_leaf_nodes(PageID pid, int& nodes, int& num_of_records, float& sum_fill, float& max_fill,float& min_fill );

};


#endif // _BTFILE_H
