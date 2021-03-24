#include <iostream>
#include <string>
#include <unordered_map>
#include <list>
#include <fstream>
#include <thread>
#include <vector>
#include <ctime>
#include <ratio>
#include <chrono>
#include <postgresql/libpq-fe.h>
#include <boost/filesystem.hpp>
#include <bits/stdc++.h>
#include <sys/stat.h>
#include <unistd.h>
#include "fileProcessor.h"
#include "ctpl.h"

#define IDENTIFIER_CACHE_SIZE 1
#define PASSWORD_CACHE_SIZE 5000000
#define DOMAIN_CACHE_SIZE 500000

#define COMMIT_RECORDS 500

namespace fs = boost::filesystem;
using namespace std;
using namespace std::chrono;

class manager
{		

	private:
		chrono::steady_clock::time_point start;
		chrono::steady_clock::time_point end;
		chrono::steady_clock::time_point fileStart;
		chrono::steady_clock::time_point fileEnd;
		std::atomic<long> totalRecordsGlobal;
		long totalRecordsCurrentFile;
		
		void getStartTime ()
		{
			this->start = chrono::steady_clock::now();
			
		}
		
		void getFileStartTime ()
		{
			this->fileStart = chrono::steady_clock::now();
			this->totalRecordsCurrentFile = this->totalRecordsGlobal;
		}
		
		void getFileEndTime ()
		{
			this->fileEnd = chrono::steady_clock::now();
			this->totalRecordsCurrentFile = this->totalRecordsGlobal - this->totalRecordsCurrentFile;
		}
		
		void getEndTime ()
		{
			this->end = chrono::steady_clock::now();
		}
		
		void printStatistics ()
		{
			long long int a,b,c,d;
			a = (this->totalRecordsGlobal*1000000);
			b = (this->totalRecordsCurrentFile*1000000);
			c = (abs((chrono::duration_cast<chrono::microseconds>(this->end - this->start).count())) + 1);
			d = (abs(chrono::duration_cast<chrono::microseconds>(this->fileEnd - this->fileStart).count()) + 1);
			if((c != 0) && (d!=0))
			{
				cout << "totalRecordsGlobal: " << this->totalRecordsGlobal << " Avg/s: " << a/c  << " thisAvg/s " << b/d << endl;
			}
			else
			{
				cout << "totalRecordsGlobal: " << this->totalRecordsGlobal << endl;
			}
		}
		
	public: 	
	
		inline bool exitNicely ()
		{
			// Looks for a file named exit.txt and terminates processing
			string fileName = "exit";
			struct stat buffer;
			return (stat (fileName.c_str(), &buffer) == 0);
		}
		
		void parse (const string &leakFilesDir, const string &encoding, int commitRecords)
		{
			std::string ext(".txt");
			fs::path dirPath (leakFilesDir);
			cin.tie(NULL);
			std::ios_base::sync_with_stdio(false);
			this->totalRecordsGlobal=0;
			this->totalRecordsCurrentFile=0;
			int temp = 0;

			long identifierCacheSize = IDENTIFIER_CACHE_SIZE;
			long passwordCacheSize = PASSWORD_CACHE_SIZE;
			long domainCacheSize = DOMAIN_CACHE_SIZE;
			
			robin_hood::unordered_map <string, long> identifierCache;
			robin_hood::unordered_map <string, long> passwordCache;
			robin_hood::unordered_map <string, long> domainCache;
			
			domainCache.reserve(domainCacheSize);
			passwordCache.reserve(passwordCacheSize);
			//identifierCache.reserve(identifierCacheSize);
			
			auto pgbackend = std::make_shared<PGBackend>();
			
			bool fileAlreadyInDB = false;
			bool processingStarted = false;
			bool processfile = false;
			
			PGconn *conn;
			conn = startDBConnection();
			
			if (conn != NULL)
			{
				if(fs::exists(dirPath) && fs::is_directory(dirPath))
				{
					for (auto const & entry : fs::recursive_directory_iterator(dirPath))
			        {
			            if (fs::is_regular_file(entry) && (entry.path().extension() == ext))
			            {	
							leakFile lF;
							lF.getDateInitiatedAt();
							lF.filePath= entry.path().c_str();
							lF.fileName = entry.path().filename().c_str();
							lF.getMD5Hash();		
							lF.size = fs::file_size(entry.path().c_str());
							leakFile *lFPtr;
							lFPtr = &lF;
							
							processfile = false;
							fileAlreadyInDB = checkIfSourceExistsInDB (conn,lF.MD5hash);
	
							if(fileAlreadyInDB)
							{
								cout << lF.fileName << " skipped: already in DB" << endl;
								processfile = false;
							}
							else
							{
								processfile = true;
							}
	
							if (processfile)
							{ 
								
								if(!processingStarted)
								{
									getStartTime();
									processingStarted = true;
								}
								
								getFileStartTime();
								cout << "[CACHE] ID: " << identifierCache.size() << "   PASSWORD: " << passwordCache.size() << "   DOMAIN " << domainCache.size() << endl;
								fileProcessor* fP = new fileProcessor(conn,&lF,encoding,commitRecords,&(this->totalRecordsGlobal),&identifierCache, &passwordCache,&domainCache,pgbackend);
								getFileEndTime();
								getEndTime();
								printStatistics();
							}
							
							// Interrupts processing
							if(exitNicely())
							{
								cout << "Exiting in 3 seconds..." << endl;
								sleep(3);
								break;
							}
							
							
							if(passwordCache.size()  > passwordCacheSize)
							{
								cout << "[CACHE] Shrinking password cache..." << endl;
								for (auto it = passwordCache.begin(); (passwordCache.size()  > 600000000);)
								{
									it = passwordCache.erase(it);
								}
							}
							if(domainCache.size()  > domainCacheSize)
							{
								cout << "[CACHE] Shrinking domain cache..." << endl;
								for (auto it = domainCache.begin(); (domainCache.size()  > 20000000);)
								{
									it = domainCache.erase(it);
								}
							}
			            }
			        }
			     }
			     else
			     {
			     	cout << "Invalid path for leak files dir" << endl;
			     	
			     }
			    finishDBConnection(conn);
			}
     
		}
		
		manager (const string &leakFilesDir)
		{
			// number of records processed before commiting
			int commitRecords = COMMIT_RECORDS;
			// size of line
			int lineSize = 500;
			const string encoding = "utf8";
			
			parse(leakFilesDir,encoding,commitRecords);
			cout << "[PARSER] Finished processing" << endl;
		}			
};
