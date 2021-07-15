/*
 * INFR11011 Minibase project
 */
#include <math.h> 
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "bufmgr.h"
#include "db.h"
#include "btfile.h"
#include "btreetest.h"

#define MAX_COMMAND_SIZE 1000

Status BTreeTest::RunTests(istream& in) {

	const char* dbname = "btdb";
	const char* logname = "btlog";
	const char* btfname = "BTreeIndex";

	remove(dbname);
	remove(logname);

	Status status;
	minibase_globals = new SystemDefs(status, dbname, logname, 1000, 500, 200);
	if (status != OK) {
		minibase_errors.show_errors();
		exit(1);
	}
	
	BTreeFile* btf = createIndex(btfname);
	if (btf == nullptr) {
		cout << "Error: Unable to create index file" << endl;
	}

	char command[MAX_COMMAND_SIZE];
	in >> command;
	while (in) {
		if (!strcmp(command, "insert")) {
			int low, high;
			in >> low >> high;
			insertHighLow(btf, low, high);
		} 
		else if (!strcmp(command, "scan")) {
			int low, high;
			in >> low >> high;
			scanHighLow(btf, low, high);
		}
		else if (!strcmp(command, "delete")) {
			int low, high;
			in >> low >> high;
			deleteHighLow(btf, low, high);
		}
		else if (!strcmp(command, "deletescan")) {
			int low, high;
			in >> low >> high;
			deleteScanHighLow(btf, low, high);
		}
		else if (!strcmp(command, "print")) {
			btf->Print();
		}
		else if (!strcmp(command, "stats")) {
			btf->DumpStatistics();
		}
		else if (!strcmp(command, "quit")) {
			break;
		}
		else {
			cout << "Error: Unrecognized command: "<<command<<endl;
		}
		in >> command;
	}

	destroyIndex(btf, btfname);

	delete minibase_globals;
	remove(dbname);
	remove(logname);

	return OK;
}


BTreeFile* BTreeTest::createIndex(const char* name) {
    cout << "Create B+tree." << endl;
    cout << "  Page size=" << MINIBASE_PAGESIZE << " Max space=" << MAX_SPACE << endl;
	
    Status status;
    BTreeFile* btf = new BTreeFile(status, name);
    if (status != OK) {
        minibase_errors.show_errors();
        cout << "  Error: cannot open index file." << endl;
		if (btf != nullptr) {
			delete btf;
			btf = nullptr;
		}
		return nullptr;
    }
    cout << "  Success." << endl;
	return btf;
}


void BTreeTest::destroyIndex(BTreeFile* btf, const char* name) {
	if (btf == nullptr) return;	
	cout << "Destroy B+tree." << endl;
	Status status = btf->DestroyFile();
	if (status != OK) {
		minibase_errors.show_errors();
	}
    delete btf;
}


void BTreeTest::insertHighLow(BTreeFile* btf, int low, int high) {
	cout << "Inserting: (" << low << " to " << high << ")" << endl;

	int numKeys = high - low + 1;
    for (int i = 0; i < numKeys; i++) {
		RecordID rid;
        rid.pageNo = i; 
        rid.slotNo = i + 1;
        
        int key = low + i;
		// Alternatively, if your implementation supports duplicates
		// int key = low + rand() % numKeys;
        
        cout << "  Insert: " << key << " @[pg,slot]=[" << rid.pageNo << "," << rid.slotNo << "]" << endl;
        if (btf->Insert(key, rid) != OK) {
			cout << "  Insertion failed." << endl;
            minibase_errors.show_errors();
			return;
        }
    }
	cout << "  Success." << endl;
}


void BTreeTest::scanHighLow(BTreeFile* btf, int low, int high) {
	cout << "Scanning (" << low << " to " << high << "):" << endl;

	int* plow = (low == -1 ? nullptr : &low);
	int* phigh = (high == -1 ? nullptr : &high);

	IndexFileScan* scan = btf->OpenScan(plow, phigh);
	if (scan == nullptr) {
		cout << "  Error: cannot open a scan." << endl;
		minibase_errors.show_errors();
		return;
	}
	
	RecordID rid;
	int ikey, count = 0;
	Status status = scan->GetNext(rid, ikey);
	while (status == OK) {
		count++;
		cout << "  Scanned @[pg,slot]=[" << rid.pageNo << "," << rid.slotNo << "]";
		cout << " key=" << ikey << endl;
		status = scan->GetNext(rid, ikey);
	}
	delete scan;
	cout << "  " << count << " records found." << endl;

	if (status != DONE) {
		minibase_errors.show_errors();
		return;
	}
	cout << "  Success." << endl;
}


void BTreeTest::deleteHighLow(BTreeFile* btf, int low, int high) {
	cout << "Deleting (" << low << "-" << high << "):" << endl;

	int* plow = (low == -1 ? nullptr : &low);
	int* phigh = (high == -1 ? nullptr : &high);

	IndexFileScan* scan = btf->OpenScan(plow, phigh);

	if (scan == nullptr) {
		cout << "Error: cannot open a scan." << endl;
		minibase_errors.show_errors();
		return;
	}

	RecordID rid;
	int ikey, count = 0;
	

	Status status = scan->GetNext(rid, ikey);

    while (status == OK) {
		cout << "  Delete [pg,slot]=[" << rid.pageNo << "," << rid.slotNo << "]";
		cout << " key=" << ikey << endl;
		delete scan;
		scan = nullptr;
		if ((status = btf->Delete(ikey, rid)) != OK) {
			cout << "  Failure to delete record...\n";
			minibase_errors.show_errors();
			break;
		}
		count++;

		scan = btf->OpenScan(plow, phigh);
		if (scan == nullptr) {
			cout << "Error: cannot open a scan." << endl;
			minibase_errors.show_errors();
			break;
		}
		status = scan->GetNext(rid, ikey);
    }
	delete scan;
	cout << "  " << count << " records deleted." << endl;

    if (status != DONE) {
		cout << "  Error: During delete";
		minibase_errors.show_errors();
		return;
    }
	cout << "  Success." << endl;
}


void BTreeTest::deleteScanHighLow(BTreeFile* btf, int low, int high) {
	cout << "Scan/Deleting (" << low << "-" << high << "):" << endl;

	int* plow = (low == -1 ? nullptr : &low);
	int* phigh = (high == -1 ? nullptr : &high);

	IndexFileScan* scan = btf->OpenScan(plow, phigh);
	if (scan == nullptr) {
		cout << "Error: cannot open a scan." << endl;
		minibase_errors.show_errors();
		return;
	}

	RecordID rid;
	int ikey, count = 0;
	Status status = scan->GetNext(rid, ikey);
    while (status == OK) {
		cout << "  DeleteCurrent [pg,slot]=[" << rid.pageNo << "," << rid.slotNo << "]";
		cout << " key=" << ikey << endl;
		if ((status = scan->DeleteCurrent()) != OK) {
			cout << "  Failure to delete record...\n";
			minibase_errors.show_errors();
			break;
		}
		count++;
		status = scan->GetNext(rid, ikey);
    }
	delete scan;
	cout << "  " << count << " records scan/deleted." << endl;

    if (status != DONE) {
		cout << "  Error: During scan/delete";
		minibase_errors.show_errors();
		return;
    }
	cout << "  Success." << endl;
}
