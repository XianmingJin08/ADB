#ifndef _INDEX_H
#define _INDEX_H

#include "minirel.h"

class IndexFileScan;

class IndexFile {

	friend IndexFileScan;
	
public:

	virtual ~IndexFile() {}; 
	    
	virtual Status Insert (const int data, const RecordID rid) = 0;
	virtual Status Delete (const int data, const RecordID rid) = 0;
	
};


class IndexFileScan {

public:

	virtual ~IndexFileScan() {} 
	
	virtual Status GetNext (RecordID &rid, int &key) = 0;
	virtual Status DeleteCurrent () = 0;
	
private:
	
};


#endif

