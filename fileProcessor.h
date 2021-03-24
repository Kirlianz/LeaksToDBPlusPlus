#include <mutex>
#include <iostream>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include <bits/stdc++.h> 
#include <chrono>
#include <boost/algorithm/string.hpp> 
#include <boost/thread/mutex.hpp> 
#include <boost/thread/thread.hpp> 
#include <postgresql/libpq-fe.h>
#include <functional>
#include <thread>
#include "leakFile.h"
#include "batch.h"
#include "ctpl.h"
#include "lineChunk.h"
#include "lineChunkProcessor.h"
#include "pgbackend.h"
#include "pgconnection.h"

#define TIPO_CPF 1
#define TIPO_CNPJ 2
#define TIPO_EMAIL 3
#define TIPO_PHONE 4
#define TIPO_CEP 5
#define TIPO_NIT 6
#define TIPO_PIS 7
#define TIPO_USERNAME 8
#define LINE_CHUNK_SIZE 250000

#define NUMBER_OF_THREADS 8

using namespace std;

stack <int> lineChunkPool;
std::mutex lineChunkMutex;
int lineChunkIdle = 0;

/*
lineChunk Chunk[27] = 	
	{lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE)};
/*	

/*
lineChunk Chunk[14] = 	
	{lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE)};
*/

/*
	lineChunk Chunk[11] = 	
	{lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE)};
*/

	lineChunk Chunk[NUMBER_OF_THREADS] = 	
	{lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE),
	lineChunk(LINE_CHUNK_SIZE)};


class fileProcessor
{
	private:
		string encoding;
		string separators[3] = {":",";"," "};
		int numLimiters = 3;
		int commitRecords;
		
	public:
	
		/*
		void initializeLineChunkPool ()
		{
			lineChunkPool.push(10);
			lineChunkPool.push(9);
			lineChunkPool.push(8);
			lineChunkPool.push(7);
			lineChunkPool.push(6);
			lineChunkPool.push(5);
			lineChunkPool.push(4);
			lineChunkPool.push(3);
			lineChunkPool.push(2);
			lineChunkPool.push(1);
			lineChunkPool.push(0);
		}
		*/
		
		/*
		void initializeLineChunkPool ()
		{
			lineChunkPool.push(26);
			lineChunkPool.push(25);
			lineChunkPool.push(24);
			lineChunkPool.push(23);
			lineChunkPool.push(22);
			lineChunkPool.push(21);
			lineChunkPool.push(20);
			lineChunkPool.push(19);
			lineChunkPool.push(18);
			lineChunkPool.push(17);
			lineChunkPool.push(16);
			lineChunkPool.push(15);
			lineChunkPool.push(14);
			lineChunkPool.push(13);
			lineChunkPool.push(12);
			lineChunkPool.push(11);
			lineChunkPool.push(10);
			lineChunkPool.push(9);
			lineChunkPool.push(8);
			lineChunkPool.push(7);
			lineChunkPool.push(6);
			lineChunkPool.push(5);
			lineChunkPool.push(4);
			lineChunkPool.push(3);
			lineChunkPool.push(2);
			lineChunkPool.push(1);
			lineChunkPool.push(0);
		}
		*/
		
		/*
		void initializeLineChunkPool ()
		{

			lineChunkPool.push(13);
			lineChunkPool.push(12);
			lineChunkPool.push(11);
			lineChunkPool.push(10);
			lineChunkPool.push(9);
			lineChunkPool.push(8);
			lineChunkPool.push(7);
			lineChunkPool.push(6);
			lineChunkPool.push(5);
			lineChunkPool.push(4);
			lineChunkPool.push(3);
			lineChunkPool.push(2);
			lineChunkPool.push(1);
			lineChunkPool.push(0);
		}
		*/
		
		void initializeLineChunkPool ()
		{
			lineChunkPool.push(7);
			lineChunkPool.push(6);
			lineChunkPool.push(5);
			lineChunkPool.push(4);
			lineChunkPool.push(3);
			lineChunkPool.push(2);
			lineChunkPool.push(1);
			lineChunkPool.push(0);
		}
		
		

		int getLineChunk ()
		{
			if(!lineChunkPool.empty())
			{
				int index = lineChunkPool.top();
				lineChunkPool.pop();
				Chunk[index].reset();
				return index;				
			}
			else
			{
				return -1;
			}
		}
			
		static void freeLineChunk(int index)
		{
			lineChunkPool.push(index);
		}
		
		static void chunkProcessThread (int id, long sourceID, int lineChunkIndex, string &encoding,std::shared_ptr<PGBackend> pgbackend, atomic<long> *sucessRecordsTotal, atomic<long> *failedRecordsTotal, atomic<long> *totalRecordsTotal, int commitRecords,robin_hood::unordered_map <string, long> *identifierCache,robin_hood::unordered_map <string, long> *passwordCache,robin_hood::unordered_map <string, long> *domainCache)
		{

			long totalRecordsLocal = 0;
			long sucessRecordsLocal = 0;
			long failedRecordsLocal = 0;

			auto conn = pgbackend->connection();
			PGconn *connT;
			connT = conn->connection().get();
			lineChunkProcessor LineChunkProcessor(sourceID,&(Chunk[lineChunkIndex]),encoding,connT,&sucessRecordsLocal,&failedRecordsLocal,&totalRecordsLocal,commitRecords,identifierCache,passwordCache,domainCache);
			*totalRecordsTotal = *totalRecordsTotal + totalRecordsLocal;
			*sucessRecordsTotal = *sucessRecordsTotal + sucessRecordsLocal;
			*failedRecordsTotal = *failedRecordsTotal + failedRecordsLocal;			
			pgbackend->freeConnection(conn);
			std::scoped_lock lock(lineChunkMutex);
			freeLineChunk(lineChunkIndex);		
		}
				
		fileProcessor (PGconn *connMain, leakFile *lF, string encoding, int commitRecords, atomic<long> *totalRecordsGlobal,robin_hood::unordered_map <string, long> *identifierCache, robin_hood::unordered_map <string, long> *passwordCache, robin_hood::unordered_map <string, long> *domainCache,std::shared_ptr<PGBackend> pgbackend)
		{	
			long sourceID = 0;
			long linesRead = 0;
			
			int lineSize = 500;
			int i = 0;
			unsigned int numberOfThreads;
			
			
			ifstream fileToRead;			
			std::atomic<long> totalRecords(0);
			std::atomic<long> sucessRecords(0);
			std::atomic<long> failedRecords(0);
			
			fileToRead.open(lF->filePath,ios::in);

			sourceID = insertSourceIntoDB(connMain,lF->fileName,lF->MD5hash,lF->dateInitiatedAt);
			
			cout << "[OPEN] file for reading: " << lF->fileName << endl;
		
			numberOfThreads = NUMBER_OF_THREADS;
			ctpl::thread_pool tP(numberOfThreads);

			string line;	
			line.reserve(lineSize);
			initializeLineChunkPool ();
			
			int lineChunkIndex = getLineChunk();

			cout << "Processing chunks of [" << commitRecords << "] lines..." << endl;
			while(fileToRead.good())
			{
				getline(fileToRead,line);
				Chunk[lineChunkIndex].addLine(line);
				linesRead++;
				if(linesRead % commitRecords == 0)
				{
					while(!(tP.n_idle() > 0) || (lineChunkPool.empty()))
					{
						
					}				
					tP.push(chunkProcessThread,sourceID,lineChunkIndex,encoding,pgbackend,&sucessRecords,&failedRecords,&totalRecords,commitRecords,identifierCache,passwordCache,domainCache);

					std::scoped_lock lock(lineChunkMutex);
					lineChunkIndex = getLineChunk();
				}
			}
			if(linesRead > 0)
			{
				while(!(tP.n_idle() > 0) || (lineChunkPool.empty()))
				{
						
				}
				
				tP.push(chunkProcessThread,sourceID,lineChunkIndex,encoding,pgbackend,&sucessRecords,&failedRecords,&totalRecords,commitRecords,identifierCache,passwordCache,domainCache);
				
			}
						
			while (tP.n_idle() < numberOfThreads)
			{
				
			}			
			
			cout << endl << "[CLOSED] file for reading: " << lF->fileName << endl << endl;
			lF->getDateCompletedAt();
			lF->totalRecords = totalRecords;
			lF->sucessRecords = sucessRecords;
			lF->failedRecords = failedRecords;
			updateSourceIntoDB (connMain, lF->MD5hash, lF->dateCompletedAt, lF->totalRecords, lF->sucessRecords, lF->failedRecords);
			*totalRecordsGlobal = *totalRecordsGlobal + totalRecords;
		}	
};
