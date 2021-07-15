#include "btfile.h"
#include "index.h"

class BTreeTest {
public:

	Status RunTests(istream& in);
	BTreeFile* createIndex(const char* name);
	void destroyIndex(BTreeFile* btf, const char* name);
	void insertHighLow(BTreeFile* btf, int low, int high);
	void scanHighLow(BTreeFile* btf, int low, int high);
	void deleteScanHighLow(BTreeFile* btf, int low, int high);
	void deleteHighLow(BTreeFile* btf, int low, int high);

};
