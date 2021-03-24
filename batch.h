#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>

using namespace std;


#ifndef BATCH_H_INCLUDED
#define BATCH_H_INCLUDED


class batch
{

	
	public:
		long size = 0;
		string stringBatch;
		long currentSize = 0;
		
		batch (long size)
		{
			this->size = size;
			this->currentSize = 0;
			this->stringBatch.reserve(this->size);
			string temp = "INSERT INTO matches (source_id,password_id,identifier_id,domain_id) values ";
			this->stringBatch = temp;
		}
		
		void reset()
		{
			this->currentSize = 0;
			string temp = "INSERT INTO matches (source_id,password_id,identifier_id,domain_id) values ";
			this->stringBatch = temp;
		}
		
		bool addMatch (std::string_view match)
		{
			this->stringBatch.append(match);
			this->currentSize++;
			return true;
		}
		
		void wrapUp ()
		{
			if (this->stringBatch.back() == ',')
			{
				this->stringBatch.pop_back();
			}
			this->stringBatch.append(" ON CONFLICT DO NOTHING;");
		}
		
		void printBatch ()
		{
			cout << stringBatch << endl;
		}
};

#endif
