/*
 * INFR11011 Minibase project
 */
#include <stdlib.h>
#include <iostream>

#include "btreetest.h"

int MINIBASE_RESTART_FLAG = 0;

int main(int argc, char *argv[]) {
	cout << "B+ tree tester" << endl;
	cout << "Execute 'btree ?' for info" << endl << endl;

	BTreeTest btt;
	
	if (argc == 1) {
		btt.RunTests(cin);
	}
	else if (argc == 2 && argv[1][0] != '?') {
		ifstream is;
		is.open(argv[1], ios::in);
		if (!is.is_open()) {
			cout << "Error: Failed to open " << argv[1] << endl;
			return 1;
		}
		btt.RunTests(is);
		is.close();
	}
	else {
		cout << "Syntax: btree [command_file]" << endl;
		cout << "If no file, commands read from stdin" << endl << endl;

		cout << "Commands should be of the form:" << endl;
		cout << "insert <low> <high>" << endl;
		cout << "scan <low> <high>" << endl;
		cout << "delete <low> <high>" << endl;
		cout << "print" << endl;
		cout << "stats" << endl;
		cout << "quit" << endl;
		cout << "Note that (<low>==-1)=>min and (<high>==-1)=>max" << endl;

		return 1;
	}
	return 0;
}
