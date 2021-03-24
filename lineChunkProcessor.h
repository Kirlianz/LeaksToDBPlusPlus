#include <iostream>
#include <string>
#include <experimental/string_view>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <mutex>
#include <unordered_map>
#include <shared_mutex>
#include <thread>
#include <chrono>
#include <bits/stdc++.h> 
#include <boost/algorithm/string.hpp> 
#include <postgresql/libpq-fe.h>
#define TIPO_CPF 1
#define TIPO_CNPJ 2
#define TIPO_EMAIL 3
#define TIPO_PHONE 4
#define TIPO_CEP 5
#define TIPO_NIT 6
#define TIPO_PIS 7
#define TIPO_USERNAME 8
#include "leakRecord.h"
#include "batch.h"
#include "lineChunk.h"
#include "robin_hood.h"
#include "pgbackend.h"
#include "pgconnection.h"

using namespace std;

std::shared_mutex identifierCacheMutex;
std::shared_mutex passwordCacheMutex;
std::shared_mutex domainCacheMutex;

class lineChunkProcessor
{
	private:
	
		string encoding;
		string separators[3] = {":",";"," "};
		int numLimiters = 3;
		int commitRecords;

	public: 	
			
		long getIdentifierIdFromCache (robin_hood::unordered_map <string, long> *identifierCache , string const &identifier)
		{
			
			std::shared_lock<std::shared_mutex> lock(identifierCacheMutex);
			robin_hood::unordered_map<string,long>::const_iterator got = identifierCache->find(identifier);
			if(got == identifierCache->end())
			{
				return 0;
			}
			else
			{
				return got->second;
			}
		}
		
		long getPasswordIdFromCache (robin_hood::unordered_map <string, long> *passwordCache , string const &password)
		{
			std::shared_lock<std::shared_mutex> lock(passwordCacheMutex);
			robin_hood::unordered_map<string,long>::const_iterator got = passwordCache->find(password);
			if(got == passwordCache->end())
			{
				return 0;
			}
			else
			{
				return got->second;
			}
		}
		
		long getDomainIdFromCache (robin_hood::unordered_map <string, long> *domainCache , string const &domain)
		{
			std::shared_lock<std::shared_mutex> lock(domainCacheMutex);
			robin_hood::unordered_map<string,long>::const_iterator got = domainCache->find(domain);
			if(got == domainCache->end())
			{
				return 0;
			}
			else
			{
				return got->second;
			}
		}
		
		bool getPasswordIDStraightFromDB (PGconn *conn, robin_hood::unordered_map <string, long> *passwordCache , leakRecord *record)
		{
			long passwordID = 0;
			
			passwordID = getPasswordIdFromDB(conn, record->passwordName);
			if(passwordID != 0)
			{
				record->passwordID = passwordID;
				return true;
			}
			else
			{
				passwordID = insertPasswordIntoDB(conn, record->passwordName);
				if(passwordID != 0)
				{
					record->passwordID = passwordID;
					return true;
				}
				else
				{
					// Retries in case of race condition
					passwordID = getPasswordIdFromDB(conn, record->passwordName);
					if(passwordID != 0)
					{
						record->passwordID = passwordID;
						return true;
					}
					else
					{
						return false;
					}
				}
			}
		}
		
		bool getIdentifierID (PGconn *conn, robin_hood::unordered_map <string, long> *identifierCache , leakRecord *record)
		{
			long identifierID = 0;
			
			
			identifierID = getIdentifierIdFromDB(conn, record->identifierName);
			if(identifierID != 0)
			{
				record->identifierID = identifierID;
				return true;
			}
			else
			{
				identifierID = insertIdentifierIntoDB(conn, record->identifierName, record->identifierType);
				if(identifierID != 0)
				{
					record->identifierID = identifierID;
					return true;
				}
				else
				{
					// Retries in case of race condition
					identifierID = getIdentifierIdFromDB(conn, record->identifierName);
					if(identifierID != 0)
					{
						record->identifierID = identifierID;
						return true;
					}
					else
					{
						return false;
					}
				}
			}
			
			/*
			identifierID = getIdentifierIdFromCache(identifierCache, record->identifierName);
			// Find identifier id on cache
			if(identifierID != 0)
			{
				// Identifier id was found
				record->identifierID = identifierID;	
				return true;
			}
			
			else(true)
			{
				identifierID = getIdentifierIdFromDB(conn, record->identifierName);
				
				if(identifierID != 0)
				{
					std::unique_lock<std::shared_mutex> lock(identifierCacheMutex);
					{
					identifierCache->insert({record->identifierName,identifierID});					
					record->identifierID = identifierID;	
					}
					return true;
				}
				
				else
				{
					// Was not found. Insert into DB
					identifierID = insertIdentifierIntoDB(conn, record->identifierName, record->identifierType);
					if(identifierID != 0)
					{
						std::unique_lock<std::shared_mutex> lock(identifierCacheMutex);
						{
						identifierCache->insert({record->identifierName,identifierID});
						record->identifierID = identifierID;	
						}
						return true;

					}
					else
					{
						return false;
					}	
					
				}
				
			}
			*/
		}
		
		bool getPasswordID (PGconn *conn, robin_hood::unordered_map <string, long> *passwordCache , leakRecord *record)
		{
			long passwordID = 0;
			
			passwordID = getPasswordIdFromCache(passwordCache, record->passwordName);
			// Achou na cache
			if(passwordID != 0)
			{
				//cout << "achou no cache" << endl;
				record->passwordID = passwordID;	
				return true;
			}
			else
			{
				// Procura no banco
				passwordID = getPasswordIdFromDB(conn, record->passwordName);
				if(passwordID != 0)
				{
					//cout << "achou no banco" << endl;
					// Insere no cache
					if(record->passwordName.length() < 10)
					{ 
						std::unique_lock<std::shared_mutex> lock(passwordCacheMutex);
						{
						passwordCache->insert({record->passwordName,passwordID});					
						}
					}
					record->passwordID = passwordID;	
					return true;
				}
				else
				{
					// Insere no banco
					passwordID = insertPasswordIntoDB(conn, record->passwordName);
					if(passwordID != 0)
					{
						if(record->passwordName.length() < 10)
						{
							// Insere na cache
							std::unique_lock<std::shared_mutex> lock(passwordCacheMutex);
							{
							passwordCache->insert({record->passwordName,passwordID});
							}
							
						}
						record->passwordID = passwordID;
						return true;
					}
					else
					{
						// tries again in case of race condition
						passwordID = getPasswordIdFromDB(conn, record->passwordName);
						if(passwordID != 0)
						{
							if(record->passwordName.length() < 10)
							{
								std::unique_lock<std::shared_mutex> lock(passwordCacheMutex);
								{
								passwordCache->insert({record->passwordName,passwordID});
								
								}	
							}
							record->passwordID = passwordID;
							return true;
						}
						else
						{
							return false;
						}

						// erro insercao banco
					}	
					
				}
				
			}
		}
		
		bool getDomainID (PGconn *conn, robin_hood::unordered_map <string, long> *domainCache , leakRecord *record)
		{
			long domainID = 0;
			
			domainID = getDomainIdFromCache(domainCache, record->domainName);
			// Achou na cache
			if(domainID != 0)
			{
				record->domainID = domainID;	
				return true;
			}
			else
			{
				// Procura no banco
				domainID = getDomainIdFromDB(conn, record->domainName);
				if(domainID != 0)
				{
					//cout << "achou no banco" << endl;
					// Insere no cache
					std::unique_lock<std::shared_mutex> lock(domainCacheMutex);
					{
					domainCache->insert({record->domainName,domainID});					
					record->domainID = domainID;	
					}
					return true;
				}
				else
				{
					// Insere no banco
					domainID = insertDomainIntoDB(conn, record->domainName);
					if(domainID != 0)
					{
						// Insere na cache
						std::unique_lock<std::shared_mutex> lock(domainCacheMutex);
						{
						domainCache->insert({record->domainName,domainID});
						record->domainID = domainID;	
						}
						return true;
					}
					else
					{
						// Tries again in case of race condition
						domainID = getDomainIdFromDB(conn, record->domainName);
						if(domainID != 0)
						{

							std::unique_lock<std::shared_mutex> lock(domainCacheMutex);
							{
							domainCache->insert({record->domainName,domainID});					
							record->domainID = domainID;	
							}
							return true;
						}
						else
						{
							return false;
						}
					}	
					
				}
				
			}
		}
		
	
		bool inline lineProcessor(std::string_view line, leakRecord *record)
		{
		    vector<string> tokens; 
		    vector<string> tokensTemp;
		    bool matchDelimiter = false;
		    
		    int i = 0;	   
		    for(i=0;(i<numLimiters && !matchDelimiter);i++)
		    {
		    	boost::split(tokens, line, boost::is_any_of(separators[i])); 
		    	
		    	if (tokens.size() == 2)
		    	{
		    		//cout << "[" << i << "]" << "tokens[0]: "<< tokens[0] << "tokens[1]: " << tokens[1] << endl;
		    		boost::split(tokensTemp, tokens[0], boost::is_any_of("@")); 
		    		if (tokensTemp.size() == 2)
	    			{	
	    				//cout << endl << "identificador: " << tokensTemp[0] << endl << "domain: " << tokensTemp[1] << endl << "password: " << tokens[1] << endl;	
	    				if(tokens[1].back() =='\r')
	    				{
	    					tokens[1].pop_back();
	    				}			
	    				record->buildLeakRecord(tokensTemp[0],tokensTemp[1],tokens[1]);
	    				matchDelimiter = true;
	    				return true;
	    			}
	    			else
	    			{
	    				// Remove Windows 'CR" character
	    				if(tokens[1].back() =='\r')
	    				{
	    					tokens[1].pop_back();
	    				}
	    				record->buildLeakRecord(tokensTemp[0],tokens[1]);
	    				matchDelimiter = true;
	    				return true;
	    			}		    	
		    	}

		    }

		    if (i==numLimiters && matchDelimiter == false)
		    {
		    	return false;
		    } 
		    
		   return false; 
		}
	
		lineChunkProcessor (long sourceID, lineChunk *lineChunk, string &encoding, PGconn *conn, long *sucessRecords,long *failedRecords, long *totalRecords, int commitRecords, robin_hood::unordered_map <string, long> *identifierCache, robin_hood::unordered_map <string, long> *passwordCache, robin_hood::unordered_map <string, long> *domainCache)
		{
		
			int batchLineSize = 200;
			int lineCount = 0;
			
			long totalRecordsLocal = 0;
			long sucessRecordsLocal = 0;
			long failedRecordsLocal=  0; 

			string line;
		
			line.reserve(500);
			
			leakRecord rec;
			leakRecord *record;
			rec.identifierName.reserve(100);
			rec.domainName.reserve(100);
			rec.passwordName.reserve(100);
			record = &rec;

			batch matchBatch(commitRecords*batchLineSize);
			istringstream lineChunkStream(lineChunk->chunk);
			
			while(lineCount < lineChunk->length)
			{
				getline(lineChunkStream,line);
				lineCount++;
			
				if(lineProcessor(line, record))
				{
					if(record->validate())
					{
						bool identifierIsOk = false;
						bool passwordIsOk = false;
						bool domainIsOk = true;
						
						identifierIsOk = getIdentifierID (conn, identifierCache, record);
						passwordIsOk = getPasswordID (conn, passwordCache, record);
						if(record->identifierType ==TIPO_EMAIL)
						{
							domainIsOk = getDomainID(conn, domainCache, record);
							if(identifierIsOk && passwordIsOk && domainIsOk)
							{
								char buffer[500];
								sprintf(buffer,"(%ld,%ld,%ld,%ld),",sourceID,record->passwordID,record->identifierID,record->domainID);					
								string matchm(buffer);					
								matchBatch.addMatch(matchm);
								sucessRecordsLocal = sucessRecordsLocal + 1;								
							}
							else
							{
								failedRecordsLocal = failedRecordsLocal + 1;
							}
							
						}
						else
						{
							if(identifierIsOk && passwordIsOk)
							{
								char buffer[500];
								sprintf(buffer,"(%ld,%ld,%ld,NULL),",sourceID,record->passwordID,record->identifierID);					
								string matchm(buffer);					
								matchBatch.addMatch(matchm);
								sucessRecordsLocal = sucessRecordsLocal + 1;
							}
							else
							{
								failedRecordsLocal = failedRecordsLocal + 1;
							}
						}
					}
					else
					{
						failedRecordsLocal = failedRecordsLocal + 1;
					}
				}
				
				else
				{
					failedRecordsLocal = failedRecordsLocal + 1;
				}
				
				totalRecordsLocal =  totalRecordsLocal + 1;
					
				if(lineChunk->length !=0)
				{
					if(totalRecordsLocal % lineChunk->length == 0)
					{
						matchBatch.wrapUp();
						commitInsertBatch(conn,matchBatch.stringBatch);
						matchBatch.reset();
					}
				}
			}
			
			if(matchBatch.currentSize > 0)
			{
				matchBatch.wrapUp();
				commitInsertBatch(conn,matchBatch.stringBatch);
			}	
			
			*sucessRecords = *sucessRecords + sucessRecordsLocal;
			*failedRecords = *failedRecords + failedRecordsLocal;
			*totalRecords = *totalRecords + totalRecordsLocal;
		}
		
};
		
