#include "frame.h"
#include "hash.h"

/**
 * Class defining the buffer replacement policy.
 */
class Replacer 
{
	public :

		Replacer();
		virtual ~Replacer();

		virtual int PickVictim() = 0;
};

class Clock : public Replacer
{
	private :
		
		int current;
		int numOfFrames;
		Frame **frames;
		HashTable *hashTable;

	public :
		
		Clock( int bufSize, Frame **frames, HashTable *hashTable );
		~Clock();
		int PickVictim();
};