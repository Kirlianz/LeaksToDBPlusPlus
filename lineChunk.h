#include <iostream>
#include <string>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
using namespace std;


#ifndef LINE_CHUNK_H_INCLUDED
#define LINE_CHUNK_H_INCLUDED

class lineChunk
{
	public:
		long size = 0;
		long length = 0;
		string chunk;
	
		lineChunk (long size)
		{
			this->size = size;
			this->length = 0;
			this->chunk = std::string();
			this->chunk.reserve(this->size);
		}
		
		void reset ()
		{
			this->length = 0;
			this->chunk = std::string();
		}
		
		void addLine (std::string_view line)
		{
			
			this->chunk.append(line);
			this->chunk.append("\n");
			this->length = this->length + 1;
		}
				
		void printLineChunk ()
		{
			cout << chunk << endl;
		}
};

#endif

