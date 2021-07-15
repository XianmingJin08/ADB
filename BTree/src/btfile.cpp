#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"


//-------------------------------------------------------------------
// BTreeFile::BTreeFile
//
// Input   : filename - filename of an index.
// Output  : returnStatus - status of execution of constructor.
//           OK if successful, FAIL otherwise.
// Purpose : If the B+ tree exists, open it.  Otherwise create a
//           new B+ tree index.
//-------------------------------------------------------------------

BTreeFile::BTreeFile (Status& returnStatus, const char* filename)
{

    PageID pid = INVALID_PAGE;
    Page *page;
    Status s = MINIBASE_DB->GetFileEntry(filename,pid); //try get the first entry of the file.
    if (s == FAIL){//file does not exist, create a file with filename.
      returnStatus = MINIBASE_BM->NewPage(pid,page); //create new page 
      if (returnStatus == OK){ //after successfully create a new page
        returnStatus = MINIBASE_DB->AddFileEntry(filename,pid); //Add the page to the file
		if (returnStatus ==OK){ //after successfully add the page to the file
			(( SortedPage *) page ) ->Init(pid); //initialise the page'id
			(( SortedPage *) page ) ->SetType(LEAF_NODE); //set the type of the page
			(( SortedPage *) page ) ->SetPrevPage(INVALID_PAGE);
			(( SortedPage *) page ) ->SetNextPage(INVALID_PAGE);
			setRootPid(pid); //set the rootid
			setFileName(filename); //set the filename
			MINIBASE_BM->UnpinPage(pid,DIRTY);
		}
      }
    }// if file exist, open the file and pin it. 	
    else{
		returnStatus = MINIBASE_BM->PinPage(pid,page);
		setRootPid(pid);	//Set the root pid to the pid of first entry of the file.
		setFileName(filename);
		MINIBASE_BM->UnpinPage(pid,DIRTY);
	}

}


//-------------------------------------------------------------------
// BTreeFile::~BTreeFile
//
// Input   : None
// Output  : None
// Purpose : Clean Up
//-------------------------------------------------------------------

BTreeFile::~BTreeFile()
{
	//unpin the root page
}


//-------------------------------------------------------------------
// BTreeFile::DestroyFile
//
// Input   : None
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Delete the entire index file by freeing all pages allocated
//           for this BTreeFile.
//-------------------------------------------------------------------

Status
BTreeFile::DestroyFile()
{
	if (rootPid == INVALID_PAGE){ // if the file is empty, nothing needs to be done;
		Status s = MINIBASE_DB->DeleteFileEntry(this->fname); //delete the file
		if (s != OK){
			cerr << "unable to delete the file " << endl;
			return FAIL;		
		}
		return s;
	}
	Status s = this->DestroyFileHelper(rootPid); //recursively remove the entire index file
	if (s !=OK ){
		cerr << "Unable to destroy the BTreeFile " << endl;
		return FAIL;
	} 
	s = MINIBASE_BM ->FreePage(rootPid); // free the page of the root
	
	if (s != OK){
		cerr << "unable to free the root " << endl;
		return FAIL;
	}
	s = MINIBASE_DB->DeleteFileEntry(this->fname); //delete the file
	if (s != OK){
		cerr << "unable to delete the file " << endl;
		return FAIL;		
	}
	return s;
}

//-------------------------------------------------------------------
// BTreeFile::DestroyFileHelper
//
// Input   : curPid 
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Recursively destroy the BTreeFile: Delete the entire index 
//			 file by freeing all pages allocated for this BTreeFile 
//-------------------------------------------------------------------

Status
BTreeFile::DestroyFileHelper(PageID curPid){
	SortedPage* curPage;
	Status s;
	PIN(curPid,curPage); //pin the current page
	if (curPid == INVALID_PAGE){
		UNPIN(curPid,CLEAN);
		return FAIL;
	}
	if (curPage->GetType()== INDEX_NODE){ //check if the current page is a index node
		BTIndexPage* ipage = (BTIndexPage *) curPage;

		PageID leftmostPid = ipage->GetLeftLink(); //free leftmost child
		Status s_l = DestroyFileHelper(leftmostPid);
		if (s_l!=OK){
			UNPIN(curPid,CLEAN);
			return FAIL;
		}
		s_l = MINIBASE_BM->FreePage(leftmostPid); // free leftmost child;
		if (s_l != OK){
				UNPIN(curPid,CLEAN);
				cerr << "unable to free the page" << endl;
				return FAIL;
		}

		int key; 		
		PageID childpid;
		RecordID rid;
		for (s = ipage->GetFirst(key,childpid,rid); s!=DONE; s= ipage->GetNext(key,childpid,rid)){
			//recursively check for each child of the page and free each child of the node
			Status tmpS = DestroyFileHelper(childpid);
			if (tmpS != OK){
				UNPIN(curPid,CLEAN);
				cerr << "DestroyFileHelper failed" << endl;
				return FAIL;
			}
			//free the page
			tmpS = MINIBASE_BM->FreePage(childpid);
			if (tmpS != OK){
				UNPIN(curPid,CLEAN);
				cerr << "unable to free the page" << endl;
				return FAIL;
			}	
		}
		UNPIN(curPid,CLEAN); //UNPIN the page
		return OK;
	}

	UNPIN(curPid,CLEAN);
	return OK;
}
//-------------------------------------------------------------------
// BTreeFile::Insert
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------


Status
BTreeFile::Insert(const int key, const RecordID rid)
{

	Status s;
	//if there is no root page, create one
	if (rootPid == INVALID_PAGE ){
		BTLeafPage* leafpage;
		RecordID outRid;		
		NEWPAGE(rootPid,leafpage);
		leafpage->Init(rootPid);
		leafpage->SetPrevPage(INVALID_PAGE);
		leafpage->SetNextPage(INVALID_PAGE);
		leafpage->SetType(LEAF_NODE);
		setRootPid(rootPid);
		leafpage->Insert(key, rid, outRid);
		UNPIN(rootPid,DIRTY);
		return OK;
	}
	
	//if there is root page
	PageID curPid = rootPid;
	bool split = false;
	int child_key;
	PageID child_pageid; 
	s = this->InsertHelper(key,rid,curPid,split,child_key,child_pageid);//recursively insert
	if (s != OK){
		return FAIL;
	}
	if (split){// if the root node needs to be split
		SortedPage* curPage;
		PIN(rootPid,curPage);
		Status s_t;
		BTIndexPage* newIndexPage;
		PageID newIndexPid;
		RecordID tmp_rid; 
		NEWPAGE(newIndexPid,newIndexPage);//create a new page and set up the info for the newpage
		if (curPage->GetType()==INDEX_NODE){
			newIndexPage->Init(newIndexPid);
			newIndexPage->SetType(INDEX_NODE);
			newIndexPage->SetNextPage(INVALID_PAGE);
			newIndexPage->SetPrevPage(INVALID_PAGE);
			newIndexPage->SetLeftLink(rootPid);
			s_t = newIndexPage->Insert(child_key,child_pageid,tmp_rid); // insert the record into the pageid
			if (s_t != OK ){
					return FAIL;
			}
			setRootPid(newIndexPid);
			UNPIN(newIndexPid,DIRTY);
		}
		else{
			newIndexPage->Init(newIndexPid);
			newIndexPage->SetType(INDEX_NODE);
			newIndexPage->SetNextPage(INVALID_PAGE);
			newIndexPage->SetPrevPage(INVALID_PAGE);
			newIndexPage->SetLeftLink(rootPid);
			s_t = newIndexPage->Insert(child_key,child_pageid,tmp_rid); // insert the record into the pageid
			if (s_t != OK ){
					return FAIL;
			}
			curPage->SetType(LEAF_NODE);
			UNPIN(rootPid,DIRTY);
			setRootPid(newIndexPid);
			UNPIN(newIndexPid,DIRTY);
		}
		
	}
    return OK;

}

//-------------------------------------------------------------------
// BTreeFile::InsertHelper
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//           curPid - the pageID of the current page.
// Output  : split - boolean, whether the child page has split occured
//           child_key - when split is true, child_key is not null 
//           child_pageid - when split is true, childpageid is not null
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------
Status
BTreeFile::InsertHelper(const int key, const RecordID rid, PageID curPid, bool& split, int& child_key, PageID& child_pageid)
{
	SortedPage* curPage;
	PageID new_curPid;
	bool new_split = false; 
	int new_child_key;
	PageID new_child_pageid;
	PIN(curPid,curPage);
	if (curPage->GetType() == INDEX_NODE){//in the page is index node
		BTIndexPage* indexpage = (BTIndexPage *) curPage;
		int dummy_key;
		RecordID dummy_rid;
		indexpage->FindPageWithKey(key, dummy_key,new_curPid,dummy_rid);//find the next page to insert 
		UNPIN(curPid,CLEAN);
		Status s = this->InsertHelper(key,rid,new_curPid,new_split,new_child_key,new_child_pageid); //recursively insert rid
		if (s != OK){
			return FAIL;
		}
		PIN(curPid,curPage);
		indexpage = (BTIndexPage *) curPage;
		if (new_split){//if needs to split the next page
			//insert the child key into the current page
			RecordID rid_tmp;
			if (indexpage->AvailableSpace()<(sizeof(IndexEntry))){//if the current page is also full
				//split the current index page
				BTIndexPage* newIndexPage;
				PageID newIndexPid;
				NEWPAGE(newIndexPid,newIndexPage);
				newIndexPage->Init(newIndexPid);
				newIndexPage->SetType(INDEX_NODE);
				newIndexPage->SetPrevPage(INVALID_PAGE);
				newIndexPage->SetNextPage(INVALID_PAGE);
				int newPageKey;
				this->Split_Index(indexpage,newIndexPage,new_child_key,new_child_pageid,newPageKey);
				split = true;
				child_key=newPageKey; //propogate the child key up to the upper page
				child_pageid=newIndexPid;
				UNPIN(newIndexPid,DIRTY);
			}
			else{
				indexpage->Insert(new_child_key,new_child_pageid,rid_tmp);
			}
		}
		UNPIN(curPid,DIRTY);
		return OK;
	}
	//case when curPage is leaf node
	BTLeafPage* leafpage = (BTLeafPage *) curPage;
	RecordID dummy; 
	if (leafpage->AvailableSpace() < (sizeof (LeafEntry))){//try to insert the key
		//case when failed to insert the key as the page is full
		BTLeafPage* newLeafPage;
		PageID newLeafPid;
		NEWPAGE(newLeafPid,newLeafPage); // create new leaf node for split
		newLeafPage->Init(newLeafPid);
		newLeafPage->SetType(LEAF_NODE);
		newLeafPage->SetNextPage(INVALID_PAGE);
		newLeafPage->SetPrevPage(INVALID_PAGE);
		Status s = this->Split_Leaf(leafpage,newLeafPage,key,rid); //split the node
		split = true;
		RecordID tmp1,tmp2;
		newLeafPage->GetFirst(child_key,tmp1,tmp2); //propogate the child key and child pageid to the higher level
		child_pageid=newLeafPid; 
		leafpage->SetNextPage(newLeafPid); //set the links between old and and new node; 
		newLeafPage->SetPrevPage(curPid);
		UNPIN(newLeafPid,DIRTY);
	}
	else{
		leafpage->Insert(key,rid,dummy);
	}
	UNPIN(curPid,DIRTY);
	return OK;
}
//-------------------------------------------------------------------
// BTreeFile::Split_Index
//
// Input   : key, pid, that need to be inserted
//           oldPage, which is full
//           newPage, the empty page
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Split the index page
//-------------------------------------------------------------------
Status 
BTreeFile::Split_Index(BTIndexPage* oldPage, BTIndexPage* newPage, const int key, PageID pid, int& newPageKey)
{
	int curKey;
	PageID curPid,curPid_tmp;
	RecordID curRid;
	bool inserted = false;	
	int curKey_tmp;
	RecordID curRid_tmp;
	Status s = oldPage->GetFirst(curKey,curPid,curRid); 
	while (s!= DONE){ //move item from old page to new page;
		Status s1 = newPage->Insert(curKey,curPid,curRid);
		if (s1 != OK){
			cerr << "insertion failed" << endl;
			return FAIL;
		}	
		curKey_tmp=curKey;
		s1 = oldPage->Delete(curKey_tmp,curRid_tmp);
		s = oldPage->GetFirst(curKey,curPid,curRid);

	}
	s = newPage->GetFirst(curKey,curPid,curRid); //moving back from new page to old page
	while (oldPage->AvailableSpace()>newPage->AvailableSpace()){
		if ( (curKey>key) && (inserted == false)){ //insert the new key to old page
			RecordID dummy;
			s = oldPage->Insert(key,pid,dummy);
			if (s != OK){
				cerr << "unable to insert" << endl;
				return FAIL;
			}
			inserted = true;
		}else{
			s = oldPage->Insert(curKey,curPid,curRid); // insert the current key to old page
			if (s != OK){
				cerr << "unable to insert" << endl;
				return FAIL;
			}
			curKey_tmp=curKey;
			curPid_tmp=curPid;
			s = newPage->Delete(curKey_tmp,curRid_tmp);
			s = newPage->GetFirst(curKey,curPid,curRid);
		} 
	}
	

	if (inserted == false){ //case when new key is not inserted, insert it to the new page
		s = newPage->Insert(key,pid,curRid);
		if (s != OK){
			cerr << "unable to insert" << endl;
			return FAIL;
		}
		//set up the left link for the newpage which is the first key in the new page and remove it from the new page;
		newPage->GetFirst(curKey,curPid,curRid);
		newPage->SetLeftLink(curPid);
		newPageKey = curKey;
		newPage->Delete(curKey,curRid);
	}else{
		//case when new key is inserted into old page; 
		//set up the left link for the newpage which is the key before curKey (curKey_tmp) and remove it from the old page;
		newPage->SetLeftLink(curPid_tmp);
		newPageKey = curKey_tmp; // the newPageKey will be the first element of 
		oldPage->Delete(curKey_tmp,curRid);
	}

	return OK;

}


//-------------------------------------------------------------------
// BTreeFile::Split_Leaf
//
// Input   : key, rid, that need to be inserted
//           oldPage, which is full
//           newPage, the empty page
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Split the leaf page
//-------------------------------------------------------------------

Status 
BTreeFile::Split_Leaf(BTLeafPage* oldPage, BTLeafPage* newPage, const int key,const RecordID rid)
{
	int curKey;
	RecordID dataRid, curRid;
	Status s = oldPage->GetFirst(curKey,dataRid,curRid);
	bool inserted = false;
	while (s!=DONE){ //move the records from oldpage to newpage
		Status s1 = newPage->Insert(curKey,dataRid,curRid);
		if (s1 != OK){
			cerr << "insertion failed" << endl;
			return FAIL;
		}
		int curKey_tmp=curKey;
		RecordID dataRid_tmp,curRid_tmp;
		dataRid_tmp=dataRid;
		s1 = oldPage->Delete(curKey_tmp,dataRid_tmp,curRid_tmp);
		s = oldPage->GetFirst(curKey,dataRid,curRid);
	}
	s = newPage->GetFirst(curKey,dataRid,curRid);
	while (oldPage->AvailableSpace()>newPage->AvailableSpace()){ //insert back the records from newpage into oldpage while keep both pages balanced 

		
		if ( (curKey>key) && (inserted == false)){ //case when insert new key and record into oldpage 
			s = oldPage->Insert(key,rid,curRid);
			if (s != OK){
				cerr << "unable to insert" << endl;
				return FAIL;
			}
			inserted = true;
		}else{
			s = oldPage->Insert(curKey,dataRid,curRid); //insert the record of the newpage into oldpage and delete the current record
			if (s != OK){
				cerr << "unable to insert" << endl;
				return FAIL;
			}
			int curKey_tmp=curKey;
			RecordID dataRid_tmp,curRid_tmp;
			dataRid_tmp=dataRid;
			s = newPage->Delete(curKey_tmp,dataRid_tmp,curRid_tmp);
			s = newPage->GetFirst(curKey,dataRid,curRid);

		} 
	}
	if (inserted == false){ //case when need to insert the new key and record into newpage 
		s = newPage->Insert(key,rid,curRid);
		if (s != OK){
			cerr << "unable to insert" << endl;
			return FAIL;
		}
	}
	
	return OK;
}



//-------------------------------------------------------------------
// BTreeFile::Delete
//
// Input   : key - the value of the key to be deleted.
//           rid - RecordID of the record to be deleted.
// Output  : None
// Return  : OK if successful, FAIL otherwise.
// Purpose : Delete an index entry with this rid and key.
// Note    : If the root becomes empty, delete it.
//-------------------------------------------------------------------

Status
BTreeFile::Delete(const int key, const RecordID rid)
{

	
	if (rootPid == INVALID_PAGE){ // if there is no root page
		return FAIL;
	}
	SortedPage * curPage,*prevPage,*nextPage;
	PageID curPid = this->rootPid;
	PIN(curPid,curPage);
	if (curPage->GetType()==LEAF_NODE){ // if the current page is leaf node

		BTLeafPage* leafPage = (BTLeafPage*) curPage;
		RecordID dataRid_test;
		Status s = leafPage->Delete(key,rid,dataRid_test);
		if (s != OK){
			return FAIL;
		}
		UNPIN(curPid,DIRTY);
		return OK;
	}
	//case when the current page is an index page
	BTIndexPage* indexPage = (BTIndexPage*) curPage;
	bool underflow = false; //the child node propogate the underflow and merged back, underflow if the child node underflow, merged if use the method merged
	bool merged = false;
	PageID pid,child_deleted_pageid,prevPid,nextPid;
	int child_key,deletedKey,curKey,nextKey;
	RecordID rid_dummy;
	Status s = indexPage->FindPageWithKeys(key,curKey,nextKey,pid,prevPid,nextPid,rid_dummy); // find the curKey, nextKey, curPid, prevPid, and nextPid using key
	if (s != OK){return FAIL;}

	s = this->DeleteHelper(key,rid,curKey,nextKey,pid,prevPid,nextPid,underflow,merged,child_key,child_deleted_pageid,deletedKey);// recursively find the key to be deleted
	if (underflow){ //case if the childnode underflows
		if (merged){//case if use the method merged 
			s=indexPage->Delete(deletedKey,rid_dummy);//delete the key needs to be deleted as merge occured
			
			if (s != OK){return FAIL;}
			int key_dummy;
			PageID pid_dummy;
			if (indexPage->GetFirst(key_dummy,pid_dummy,rid_dummy) == DONE){
				UNPIN(curPid,DIRTY);
				setRootPid(indexPage->GetLeftLink());
				return OK;
			}
		}else{//case if use redistribution method, delete the key in the index node and insert and new key based on the child key propogated back
			s=indexPage->Delete(deletedKey,rid_dummy);
			if (s != OK){return FAIL;}
			s=indexPage->Insert(child_key,child_deleted_pageid,rid_dummy);	
		}
	}

	UNPIN(curPid,DIRTY);
    return OK;
}

//-------------------------------------------------------------------
// BTreeFile::DeleteHelper
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//           curPid - the pageID of the current page.
// Output  : split - boolean, whether the child page has split occured
//           child_key - when split is true, child_key is not null 
//           child_deleted_pageid - when split is true, childpageid is not null
//           child_deleted -  when split is true, child_deleted is not null
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------
Status 
BTreeFile::DeleteHelper(const int key, const RecordID rid, const int curKey,const int nextKey,PageID curPid, PageID prevPid, PageID nextPid,bool& underflow,bool& merged, int& child_key, PageID& child_pageid,int& deletedKey)
{

	SortedPage * curPage, *prevPage,*nextPage;
	PIN(curPid,curPage);

	if (curPage->GetType() == INDEX_NODE){//case when the curpage is index node
		BTIndexPage* indexPage = (BTIndexPage *) curPage;
		bool new_underflow = false;
		bool new_merged = false;
	
		PageID new_pid,new_child_pageid,new_prevPid,new_nextPid;
		int new_child_key,new_curKey,new_nextKey,new_deletedKey;
		RecordID rid_dummy;
		indexPage->FindPageWithKeys(key,new_curKey,new_nextKey,new_pid,new_prevPid,new_nextPid,rid_dummy);//find the prev,cur and next page where key belongs;
		if (new_prevPid == new_pid) {
			new_prevPid = INVALID_PAGE;
		}
		if (new_nextPid == new_pid){
			new_nextPid = INVALID_PAGE;
		}
		Status s = this->DeleteHelper(key,rid,new_curKey,new_nextKey,new_pid,new_prevPid,new_nextPid,new_underflow,new_merged,new_child_key,new_child_pageid,new_deletedKey);
		if (s!=OK){return FAIL;}
		if (new_underflow){//case when underflow occurs in the child pages
			if (new_merged){//check the methods used to solve the underflow, merge or redistribution.
				//case merge occursed
				s=indexPage->Delete(new_deletedKey,rid_dummy);//delete the key needs to be deleted as merge occured
				if (s != OK){return FAIL;}
				if (! indexPage->IsAtLeastHalfFull()){//check if the deletion cause the current page to be underflow
					//case when the page is underflow
					underflow = true;
					bool reconstruc = false;

					if (prevPid != INVALID_PAGE){//borrow a key from previous page
						PIN(prevPid,prevPage);
						BTIndexPage* prevIndex = (BTIndexPage *) prevPage;
						if (prevIndex->IsAtLeastHalfFullAfterDelete()){
							//reconstruction
							int key_tmp;
							PageID pid_tmp;
							RecordID rid_tmp;
							Status s = prevIndex->GetLast(key_tmp,pid_tmp,rid_tmp);
							if (s!=OK){return FAIL;}
							s = prevIndex->Delete(key_tmp,rid_tmp);
							if (s!=OK){return FAIL;}
							s = indexPage->Insert(curKey,indexPage->GetLeftLink(),rid_tmp);
							if (s!=OK){return FAIL;}
							indexPage->SetLeftLink(pid_tmp);	
							reconstruc = true;
							child_key =	key_tmp;
							child_pageid=curPid;
							deletedKey = curKey;
							UNPIN(prevPid,DIRTY);
						}else{
							UNPIN (prevPid,CLEAN);
						}
						
					}
					if (nextPid != INVALID_PAGE && !reconstruc){//borrow a key from next page
						PIN(nextPid,nextPage);
						BTIndexPage* nextIndex = (BTIndexPage *) nextPage;
						if (nextIndex->IsAtLeastHalfFullAfterDelete()){
							int key_tmp;
							PageID pid_tmp;
							RecordID rid_tmp;
							Status s = nextIndex->GetFirst(key_tmp,pid_tmp,rid_tmp);
							if (s!=OK){return FAIL;}
							s = nextIndex->Delete(key_tmp,rid_tmp);
							if (s!=OK){return FAIL;}
							s = indexPage->Insert(nextKey,nextIndex->GetLeftLink(),rid_dummy);
							if (s!=OK){return FAIL;}
							nextIndex->SetLeftLink(pid_tmp);
							reconstruc = true;
							child_key =	key_tmp;
							child_pageid=nextPid;
							deletedKey = nextKey;
							UNPIN(nextPid,DIRTY);
						}else{
							UNPIN(nextPid,CLEAN);
						}
						
					}
					if (!reconstruc){//case when need to merge
						merged = true;
						if (nextPid != INVALID_PAGE){//merge the next page into current page
							PIN(nextPid,nextPage);
							BTIndexPage* nextIndex = (BTIndexPage *) nextPage;
							
							int key_tmp;
							PageID pid_tmp;
							RecordID rid_tmp;
							Status s = nextIndex->GetFirst(key_tmp,pid_tmp,rid_tmp);
							if (s!=OK){return FAIL;}
							while (s != DONE){
								s = indexPage->Insert(key_tmp,pid_tmp,rid_tmp);
								s = nextIndex->Delete(key_tmp,rid_tmp);
								s = nextIndex->GetFirst(key_tmp,pid_tmp,rid_tmp);
							}
							indexPage->Insert(nextKey,nextIndex->GetLeftLink(),rid_dummy);
							UNPIN (nextPid,DIRTY);
							deletedKey = nextKey;
						}
						else{//merge the current page into prev page
							PIN(prevPid,prevPage);
							BTIndexPage* prevIndex = (BTIndexPage *) prevPage;
							
							int key_tmp;
							PageID pid_tmp;
							RecordID rid_tmp;
							Status s = indexPage->GetFirst(key_tmp,pid_tmp,rid_tmp);
							if (s!=OK){return FAIL;}
							while (s != DONE){
								s = prevIndex->Insert(key_tmp,pid_tmp,rid_tmp);
								s = indexPage->Delete(key_tmp,rid_tmp);
								s = indexPage->GetFirst(key_tmp,pid_tmp,rid_tmp);
							}
							prevIndex->Insert(curKey,indexPage->GetLeftLink(),rid_tmp);
							UNPIN (prevPid,DIRTY);
							deletedKey = curKey;
						}
					}
				}

			}
			else{
				s=indexPage->Delete(new_deletedKey,rid_dummy);
				if (s != OK){return FAIL;}
				s=indexPage->Insert(new_child_key,new_child_pageid,rid_dummy);				
			}
		}
		UNPIN(curPid,DIRTY);
		return OK;
	}
	//case when curpage is leaf node
	BTLeafPage* leafPage = (BTLeafPage *) curPage;
	RecordID datarid_test;
	Status s = leafPage->Delete(key,rid,datarid_test);
	if (s != OK){
		return FAIL;
	}
	if (! leafPage->IsAtLeastHalfFull()){//case when the leaf node is not half full
		underflow = true;
		bool reconstruc = false;
		if (prevPid != INVALID_PAGE){//borrow the rid from previous page
			PIN(prevPid,prevPage);
			BTLeafPage* prevLeaf = (BTLeafPage *) prevPage;
			if (prevLeaf->IsAtLeastHalfFullAfterDelete()){
				this->DeleteLeaf_prev(prevLeaf,leafPage,child_key);
				UNPIN(prevPid,DIRTY);
				reconstruc = true;
				child_pageid=curPid;
				deletedKey = curKey;
			}else{
				UNPIN (prevPid,CLEAN);
			}
			
		}
		if (nextPid != INVALID_PAGE && !reconstruc){//borrow the rid from next page
			PIN(nextPid,nextPage);			
			BTLeafPage* nextLeaf = (BTLeafPage *) nextPage;
			if (nextLeaf->IsAtLeastHalfFullAfterDelete()){
				this->DeleteLeaf_next(nextLeaf,leafPage,child_key);
				reconstruc = true;
				child_pageid=nextPid;
				deletedKey = nextKey;
				UNPIN(nextPid,DIRTY);
			}else{
				UNPIN(nextPid,CLEAN)
			}
			
		}
		if (!reconstruc){//if neibourgh nodes are not avaliable
			merged = true;
			if (nextPid != INVALID_PAGE){//if the current page is not the rightmost page, merge curpage and next page
				PIN(nextPid,nextPage);
				BTLeafPage* nextLeaf = (BTLeafPage *) nextPage;
				PageID nextLink = nextLeaf->GetNextPage();
				this->MergeLeaf_next(nextLeaf,leafPage);
				leafPage->SetNextPage(nextLink);
				deletedKey = nextKey;
				UNPIN (nextPid,DIRTY);
			}
			else {
				PIN(prevPid,prevPage);//if the current page is the rightmost page, merge curpage with prev page
				BTLeafPage* prevLeaf = (BTLeafPage *) prevPage;
				PageID nextLink = leafPage->GetNextPage();
				this->MergeLeaf_prev(prevLeaf,leafPage);
				prevLeaf->SetNextPage(nextLink);
				deletedKey = curKey;
				UNPIN (prevPid,DIRTY);
			}
			
		}		
	}
	UNPIN(curPid,DIRTY);
	return OK;
}


//-------------------------------------------------------------------
// BTreeFile::DeleteLeaf
//
// Input   : key - the value of the key to be inserted.
//           rid - RecordID of the record to be inserted.
//           curPid - the pageID of the current page.
// Output  : split - boolean, whether the child page has split occured
//           child_key - when split is true, child_key is not null 
//           child_pageid - when split is true, childpageid is not null
// Return  : OK if successful, FAIL otherwise.
// Purpose : Insert an index entry with this rid and key.
// Note    : If the root didn't exist, create it.
//-------------------------------------------------------------------

Status
BTreeFile::DeleteLeaf_prev(BTLeafPage* prevPage, BTLeafPage* curPage, int& newKey){
	int key_tmp;
	RecordID dataRid_tmp, rid_tmp;
	Status s = prevPage->GetLast(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	s = prevPage->Delete(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	s = curPage->Insert(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	s = curPage->GetFirst(newKey,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	return OK;
}
Status
BTreeFile::DeleteLeaf_next(BTLeafPage* nextPage, BTLeafPage* curPage, int& newKey){
	int key_tmp;
	RecordID dataRid_tmp, rid_tmp;
	Status s = nextPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	s = nextPage->Delete(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	s = curPage->Insert(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	s = nextPage->GetFirst(newKey,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	return OK;
}
Status
BTreeFile::MergeLeaf_next(BTLeafPage* nextPage, BTLeafPage* curPage){
	int key_tmp;
	RecordID dataRid_tmp, rid_tmp;
	Status s = nextPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	while (s != DONE){
		s = curPage->Insert(key_tmp,dataRid_tmp,rid_tmp);
		s = nextPage->Delete(key_tmp,dataRid_tmp,rid_tmp);
		s = nextPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp);
	}
	return OK;
}
Status
BTreeFile::MergeLeaf_prev(BTLeafPage* prevPage, BTLeafPage* curPage){
	int key_tmp;
	RecordID dataRid_tmp, rid_tmp;
	Status s = curPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp);
	if (s!=OK){return FAIL;}
	while (s != DONE){
		s = prevPage->Insert(key_tmp,dataRid_tmp,rid_tmp);
		s = curPage->Delete(key_tmp,dataRid_tmp,rid_tmp);
		s = curPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp);
	}
	return OK;
}
//-------------------------------------------------------------------
// BTreeFile::OpenScan
//
// Input   : lowKey, highKey - pointer to keys, indicate the range
//                             to scan.
// Output  : None
// Return  : A pointer to IndexFileScan class.
// Purpose : Initialize a scan.
// Note    : Usage of lowKey and highKey :
//
//           lowKey      highKey      range
//			 value	     value
//           --------------------------------------------------
//           nullptr     nullptr      whole index
//           nullptr     !nullptr     minimum to highKey
//           !nullptr    nullptr      lowKey to maximum
//           !nullptr    =lowKey  	  exact match
//           !nullptr    >lowKey      lowKey to highKey
//-------------------------------------------------------------------

IndexFileScan*
BTreeFile::OpenScan(const int* lowKey, const int* highKey)
{
	BTreeFileScan* scan = new BTreeFileScan();	
	scan->btfile= this;
    if (rootPid == INVALID_PAGE){
		scan->lowKey = *lowKey;
		scan->highKey = *highKey;
		scan->curPid = INVALID_PAGE;
		scan->s = DONE;
		return scan;
	}
	if (lowKey == nullptr){
		int height_tmp,key_tmp;
		if (highKey != nullptr){
			scan->highKey = *highKey;
		}
		else{
			this->GetMaxKey(key_tmp);
			scan->highKey = key_tmp;
		}
		scan->curPid = this->GetMinimumPid(key_tmp,height_tmp);
		
		scan->lowKey = key_tmp;
		scan->s=OK;

		return scan;
	}
	int height_tmp,key_tmp;
	scan->lowKey = *lowKey;
	if (highKey != nullptr){
		scan->highKey = *highKey;
	}
	else{
		this->GetMaxKey(key_tmp);
		scan->highKey = key_tmp;
	}
	scan->curPid=this->FindPidWithKey(*lowKey);
	scan->s=OK;
	return scan;

}
//-------------------------------------------------------------------
//BTreeFile:: FindPidWithKey(const int key)
//-------------------------------------------------------------------
PageID
BTreeFile::FindPidWithKey(const int key){
	PageID curPid = rootPid;
	if (curPid == INVALID_PAGE){
		return INVALID_PAGE;
	}
	SortedPage* curPage;
	PIN(curPid,curPage);
	if (curPage->GetType() == LEAF_NODE){
		UNPIN(curPid,CLEAN);
		return curPid;
	}
	while (curPage->GetType() != LEAF_NODE){
		BTIndexPage* indexPage = (BTIndexPage *) curPage;
		
		int key_tmp;
		PageID pid_tmp;
		RecordID rid_tmp;
		indexPage->FindPageWithKey(key,key_tmp,pid_tmp,rid_tmp);
		UNPIN(curPid,CLEAN);
		curPid = pid_tmp;
		PIN(curPid,curPage);

	}
	UNPIN(curPid,CLEAN);
	return curPid;
}
//-------------------------------------------------------------------
// BTreeFile::GetMinimumPid
//
// Input   : None
// Output  : None
// Return  : the minimum Pid which is the leftmost child
// Purpose : return the pid of the leftmost child
//-------------------------------------------------------------------
PageID 
BTreeFile::GetMinimumPid(int & key, int & height ){
	PageID curPid = this->rootPid;
	if (curPid == INVALID_PAGE){
		height = -1;
		return curPid;
	}
	SortedPage* curPage;
	PIN(curPid,curPage);
	height = 0;
	while (curPage->GetType() !=LEAF_NODE){
		BTIndexPage* indexPage = (BTIndexPage *) curPage;
		PageID pid_tmp = indexPage->GetLeftLink();
		UNPIN(curPid,CLEAN);
		curPid = pid_tmp;
		height = height+1;
		PIN(curPid,curPage);
	}
	BTLeafPage* leafPage = (BTLeafPage *) curPage;
	RecordID dataRid_tmp,rid_tmp;
	leafPage->GetFirst(key,dataRid_tmp,rid_tmp);
	UNPIN(curPid,CLEAN);
	return curPid;
}

PageID 
BTreeFile::GetMaxKey(int & key){
	PageID curPid = this->rootPid;
	if (curPid == INVALID_PAGE){
		return curPid;
	}
	SortedPage* curPage;
	PIN(curPid,curPage);
	while (curPage->GetType() !=LEAF_NODE){
		PageID pid_tmp;
		int key_tmp;
		RecordID rid_tmp;
		BTIndexPage* indexPage = (BTIndexPage *) curPage;
		indexPage->GetLast(key_tmp,pid_tmp,rid_tmp);
		UNPIN(curPid,CLEAN);
		curPid = pid_tmp;
		PIN(curPid,curPage);
	}
	BTLeafPage* leafPage = (BTLeafPage *) curPage;
	RecordID dataRid_tmp,rid_tmp;
	leafPage->GetLast(key,dataRid_tmp,rid_tmp);
	UNPIN(curPid,CLEAN);
	return curPid;
}
//-------------------------------------------------------------------
// BTreeFile::PrintTree
//
// Input   : pageID - root of the tree to print.
// Output  : None
// Return  : None
// Purpose : Print out the content of the tree rooted at pid.
//-------------------------------------------------------------------

Status
BTreeFile::PrintTree(PageID pageID)
{
	if ( pageID == INVALID_PAGE ) {
    	return FAIL;
	}

	SortedPage* page = nullptr;
	PIN(pageID, page);

	NodeType type = (NodeType) page->GetType();
	if (type == INDEX_NODE)
	{
		BTIndexPage* index = (BTIndexPage *) page;
		PageID curPageID = index->GetLeftLink();
		PrintTree(curPageID);

		RecordID curRid;
		int key;
		Status s = index->GetFirst(key, curPageID, curRid);
		while (s != DONE)
		{
			PrintTree(curPageID);
			s = index->GetNext(key, curPageID, curRid);
		}
	}

	UNPIN(pageID, CLEAN);
	PrintNode(pageID);

	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::PrintNode
//
// Input   : pageID - the node to print.
// Output  : None
// Return  : None
// Purpose : Print out the content of the node pid.
//-------------------------------------------------------------------

Status
BTreeFile::PrintNode(PageID pageID)
{
	SortedPage* page = nullptr;
	PIN(pageID, page);

	NodeType type = (NodeType) page->GetType();
	switch (type)
	{
		case INDEX_NODE:
		{
			BTIndexPage* index = (BTIndexPage *) page;
			PageID curPageID = index->GetLeftLink();
			cout << "\n---------------- Content of index node " << pageID << "-----------------------------" << endl;
			cout << "\n Left most PageID:  "  << curPageID << endl;

			RecordID currRid;
			int key, i = 0;

			Status s = index->GetFirst(key, curPageID, currRid);
			while (s != DONE)
			{
				i++;
				cout <<  "Key: " << key << "	PageID: " << curPageID << endl;
				s = index->GetNext(key, curPageID, currRid);
			}
			cout << "\n This page contains  " << i << "  entries." << endl;
			break;
		}

		case LEAF_NODE:
		{
			BTLeafPage* leaf = (BTLeafPage *) page;
			cout << "\n---------------- Content of leaf node " << pageID << "-----------------------------" << endl;

			RecordID dataRid, currRid;
			int key, i = 0;

			Status s = leaf->GetFirst(key, dataRid, currRid);
			while (s != DONE)
			{
				i++;
				cout << "DataRecord ID: " << dataRid << " Key: " << key << endl;
				s = leaf->GetNext(key, dataRid, currRid);
			}
			cout << "\n This page contains  " << i << "  entries." << endl;
			break;
		}
	}
	UNPIN(pageID, CLEAN);

	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::Print
//
// Input   : None
// Output  : None
// Return  : None
// Purpose : Print out this B+ Tree
//-------------------------------------------------------------------

Status
BTreeFile::Print()
{
	cout << "\n\n-------------- Now Begin Printing a new whole B+ Tree -----------" << endl;

	if (PrintTree(rootPid) == OK)
		return OK;

	return FAIL;
}

//-------------------------------------------------------------------
// BTreeFile::DumpStatistics
//
// Input   : None
// Output  : None
// Return  : None
// Purpose : Print out the following statistics.
//           1. Total number of leaf nodes, and index nodes.
//           2. Total number of leaf entries.
//           3. Total number of index entries.
//           4. Mean, Min, and max fill factor of leaf nodes and
//              index nodes.
//           5. Height of the tree.
//-------------------------------------------------------------------
Status
BTreeFile::DumpStatistics()
{
	// TODO: add your code here
	//print out the total number of nodes;
	SortedPage * rootPage;
	PIN(rootPid,rootPage);
	if (rootPid == INVALID_PAGE){
		cout << "\n---------------- tree is empty -----------------------------" << endl;	
		UNPIN (rootPid,CLEAN)
		return OK;
	}
	if (rootPage->GetType() == LEAF_NODE){
		int num_leaf_nodes = 0;
		int num_leaf_entries=0;
		float leaf_sum_fill =0;
		float leaf_max_fill =0;
		float leaf_min_fill =0;
		int height;
		this->Sum_leaf_nodes(rootPid,num_leaf_nodes,num_leaf_entries,leaf_sum_fill,leaf_max_fill,leaf_min_fill);
		cout << "\n---------------- Total number of leaf nodes: " << 1 << "-----------------------------" << endl;	
		cout << "\n---------------- Total number of index nodes: " << 0 << "-----------------------------" << endl;	
		cout << "\n---------------- Total number of leaf entries: " << num_leaf_entries << "-----------------------------" << endl;	
		cout << "\n---------------- Total number of index entries " << 0 << "-----------------------------" << endl;	
		cout << "\n---------------- Fill for Leaf nodes: mean = " << leaf_sum_fill/num_leaf_nodes << "min = " << leaf_min_fill << "max = " <<leaf_max_fill << "-----------------------------" << endl;
		cout << "\n---------------- Fill for index nodes: not applicable" << endl;
		cout << "\n---------------- Height of the tree: " << 0 << "-----------------------------" << endl;	
		UNPIN (rootPid,CLEAN);
		return OK;
	}
	int num_leaf_nodes = 0;
	int num_leaf_entries=0;
	float leaf_sum_fill =0;
	float leaf_max_fill =0;
	float leaf_min_fill =1;
	int num_index_nodes = 0;
	int num_index_entries=0;
	float index_sum_fill =0;
	float index_max_fill =0;
	float index_min_fill =1;
	int height;
	int key_tmp;
	this->Sum_Index_nodes(rootPid,num_index_nodes,num_index_entries,index_sum_fill,index_max_fill,index_min_fill);
	PageID firstLeaf = this->GetMinimumPid(key_tmp,height);
	this->Sum_leaf_nodes(firstLeaf,num_leaf_nodes,num_leaf_entries,leaf_sum_fill,leaf_max_fill,leaf_min_fill);
	cout << "\n---------------- Total number of nodes: " << num_leaf_nodes+num_index_nodes << "-----------------------------" << endl;	
	cout << "\n---------------- Total number of leaf nodes: " << num_leaf_nodes << "-----------------------------" << endl;	
	cout << "\n---------------- Total number of index nodes: " << num_index_nodes << "-----------------------------" << endl;	
	cout << "\n---------------- Total number of leaf entries: " << num_leaf_entries << "-----------------------------" << endl;	
	cout << "\n---------------- Total number of index entries " << num_index_entries << "-----------------------------" << endl;	
	cout << "\n---------------- Fill for Leaf nodes: mean = " << leaf_sum_fill/num_leaf_nodes << "min = " << leaf_min_fill << "max = " << leaf_max_fill << "-----------------------------" << endl;
	cout << "\n---------------- Fill for Index nodes: mean = " << index_sum_fill/num_index_nodes << "min = " << index_min_fill << "max = " << index_max_fill << "-----------------------------" << endl;
	cout << "\n---------------- Height of the tree: " << height << "-----------------------------" << endl;	
	UNPIN (rootPid,CLEAN);	

	return OK;
}

//-------------------------------------------------------------------
// BTreeFile::Sum_Index_nodes
//-------------------------------------------------------------------
Status 
BTreeFile::Sum_Index_nodes(PageID curPid, int& nodes, int& num_entries, float& sum_fill, float& max_fill,float& min_fill)
{	
	SortedPage * curPage;
	PIN(curPid,curPage);
	if (curPage->GetType()==LEAF_NODE){
		num_entries =0;
		UNPIN(curPid,CLEAN);
		return OK;
	}
	
	int new_num_entries;
	int key_tmp;
	PageID pid_tmp;
	RecordID rid_tmp;
	BTIndexPage* indexPage = (BTIndexPage*)curPage;
	Status s = indexPage->GetFirst(key_tmp,pid_tmp,rid_tmp);
	num_entries = 0;
	Sum_Index_nodes(indexPage->GetLeftLink(),nodes,new_num_entries,sum_fill,max_fill,min_fill);
	nodes = nodes+1;
	num_entries = num_entries + new_num_entries ;

	float fill = (float) 1 - (indexPage->AvailableSpace() / (float) HEAPPAGE_DATA_SIZE);
	sum_fill = sum_fill +fill;
	if (fill > max_fill){
		max_fill = fill;
	}
	if (fill< min_fill){
		min_fill = fill;
	}

	while (s != DONE){
		num_entries = num_entries +1;
		Sum_Index_nodes(pid_tmp,nodes,new_num_entries,sum_fill,max_fill,min_fill);
		num_entries = num_entries + new_num_entries;
		s = indexPage->GetNext(key_tmp,pid_tmp,rid_tmp);
	}
	UNPIN(curPid,CLEAN);
	return OK;
}
//-------------------------------------------------------------------
// BTreeFile::Sum_leaf_nodes
//-------------------------------------------------------------------
Status 
BTreeFile::Sum_leaf_nodes(PageID pid, int& nodes, int& num_of_records, float& sum_fill, float& max_fill,float& min_fill ){
	BTLeafPage * leafPage;
	PIN(pid,leafPage);
	int key_tmp;
	RecordID dataRid_tmp, rid_tmp;
	Status s = leafPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp);
	nodes = nodes+1;

	while (s!=DONE){
		num_of_records = num_of_records+1;
		s = leafPage->GetNext(key_tmp,dataRid_tmp,rid_tmp);
	}
	float fill = (float) 1 - (leafPage->AvailableSpace() / (float) HEAPPAGE_DATA_SIZE);
	sum_fill = sum_fill +fill;

	if (fill > max_fill){
		max_fill = fill;
	}
	if (fill< min_fill){
		min_fill = fill;
	}
	PageID nextPid = leafPage->GetNextPage();
	UNPIN(pid,CLEAN);
	if ( nextPid != INVALID_PAGE){
		Sum_leaf_nodes(nextPid,nodes,num_of_records,sum_fill,max_fill,min_fill);
	}
	
	return OK;
}