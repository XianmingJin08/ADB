
#ifndef _BUF_H
#define _BUF_H

#include "db.h"
#include "page.h"
#include "frame.h"
#include "replacer.h"
#include "hash.h"


class BufMgr 
{
	private:

		/*
		 * hashTable to give hash access to frames
		 */
		HashTable *hashTable;
		Frame **frames; 			// pool of frames
		
		/*
		 * Component responsible of implementing the buffer replacement policy 
		 */
		Replacer *replacer;
		unsigned int numOfFrames;			// number of frames

		int FindFrame( PageID pid );
		long totalCall;				// number of times upper layers try to pin a page
		long totalHit;		     	// number of times upper layers try to pin a page and the page is already in the buffer

	public:

		BufMgr( unsigned int bufSize );
		~BufMgr();      
		Status PinPage( PageID pid, Page*& page, bool emptyPage = false );
		Status UnpinPage( PageID pid, bool dirty = false );
		Status NewPage( PageID& pid, Page*& firstPage, int howMany = 1 ); 
		Status FreePage( PageID pid ); 
		Status FlushPage( PageID pid );
		Status FlushAllPages();
		Status GetStat(long& pinNo, long& missNo) { pinNo = totalCall; missNo = totalCall-totalHit; return OK; }

		unsigned int GetNumOfFrames();
		unsigned int GetNumOfUnpinnedFrames();

		void PrintStat();
		void ResetStat() { totalHit = 0; totalCall = 0; }
};

#endif // _BUF_H
