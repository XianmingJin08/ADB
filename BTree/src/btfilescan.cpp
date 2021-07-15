#include "minirel.h"
#include "bufmgr.h"
#include "db.h"
#include "new_error.h"
#include "btfile.h"
#include "btfilescan.h"

//-------------------------------------------------------------------
// BTreeFileScan::~BTreeFileScan
//
// Input   : None
// Output  : None
// Purpose : Clean up the B+ tree scan.
//-------------------------------------------------------------------

BTreeFileScan::~BTreeFileScan()
{
    // TODO: add your code here
}


//-------------------------------------------------------------------
// BTreeFileScan::GetNext
//
// Input   : None
// Output  : rid  - record id of the scanned record.
//           key  - key of the scanned record
// Purpose : Return the next record from the B+-tree index.
// Return  : OK if successful, DONE if no more records to read.
//-------------------------------------------------------------------

Status 
BTreeFileScan::GetNext(RecordID& rid, int& key)
{
    if (this->s == DONE){
        return this->s;
    }
    PageID pid = this->curPid;
    if (pid == INVALID_PAGE){
        this-> s = DONE;
        return this->s;
    }
    Status s = this->GetNextHelper(pid,rid,key);
    if (s == DONE){
        this->btfile->DumpStatistics();
    }
    return s;
}

Status
BTreeFileScan::GetNextHelper(PageID pid, RecordID & rid, int& key){

    if (pid == INVALID_PAGE){  //if the current page is empty, return DONE
        this->s = DONE;
        return DONE;
    }
    if (this->lowKey > this->highKey){ //if the lowerKey is higher than the higher key, return DONE
        this->s = DONE;
        return DONE;
    }
    SortedPage* curPage;
    int key_tmp,key_last;
    RecordID dataRid_tmp, rid_tmp;
    //BTLeafPage* curPage;
    PIN(pid,curPage);
    BTLeafPage* leafPage = (BTLeafPage* ) curPage;
    if (leafPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp)== DONE){
        this->s = DONE;
        return DONE;
    }

    leafPage->GetLast(key_last,dataRid_tmp,rid_tmp); 
 
    if (key_last<this->lowKey || key_last == key_scanned){// if the last key is lower than LoKey on this page, or it is scanned search next page
        PageID nextPid = leafPage->GetNextPage();
        UNPIN(pid,CLEAN);
        this->curPid = nextPid;
        this->s = GetNextHelper(nextPid,rid,key);
        return this->s;
    }
    
    // if the key lower geater than or equal to loKey:
    leafPage->GetFirst(key_tmp,dataRid_tmp,rid_tmp); // if the high key is smaller than the first key in the page, return DONE
    while (this->lowKey>key_tmp || key_tmp == this->key_scanned){ //if the lowkey is higher than the current key or the key is scanned, go to the next key
        if (key_tmp == key_last) {break;}
        leafPage->GetNext(key_tmp,dataRid_tmp,rid_tmp);
    }


    if (this->highKey<key_tmp){ //if the cur key is higher the highKey, return DONE
        UNPIN(pid,CLEAN);
        this->s=DONE;
        return DONE;
    }

    rid = dataRid_tmp;  //return the current key and update member info.
    key = key_tmp;
    lowKey = key_tmp;
    this->s = OK;
    this->dataRid = dataRid_tmp;
    this->key_scanned = key_tmp;
    if (key_tmp == key_last){ // if the key is the last key, 
        PageID nextPid = leafPage->GetNextPage();
        UNPIN(pid,CLEAN);
        this->curPid = nextPid;
        this->s = OK;
        return OK;;
    }
    UNPIN(pid,CLEAN); 
    return OK;
}
//-------------------------------------------------------------------
// BTreeFileScan::DeleteCurrent
//
// Input   : None
// Output  : None
// Purpose : Delete the entry currently being scanned (i.e. returned
//           by previous call of GetNext())
// Return  : OK if successful, DONE if no more record to read.
//-------------------------------------------------------------------


Status 
BTreeFileScan::DeleteCurrent()
{  
    Status s; //create a BTreefile object to delete the current record
    RecordID rid_tmp;
    s =this->btfile->Delete(this->key_scanned,this->dataRid);
    if (s != OK){
        return DONE;
    }
    return OK;
}


