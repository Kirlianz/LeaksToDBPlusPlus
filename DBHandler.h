#include <iostream>
#include <stdio.h>
#include <postgresql/libpq-fe.h>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include <bits/stdc++.h> 
#include <string>
#include <sys/stat.h>

void finishDBConnection(PGconn *conn)
{ 
    PQfinish(conn);
}

PGconn* startDBConnection ()
{
	// conn info goes here
	PGconn *conn = PQconnectdb("");
	
	if (PQstatus(conn) == CONNECTION_BAD)
    {
    	cout << "Connection to database failed:" << endl;
    	PQerrorMessage(conn);
        finishDBConnection(conn);
        return NULL; 
    }
    else
    {

    	return conn;
    }
}

long getIdentifierIdFromDB(PGconn *conn, const string &identifierName)
{
 	const int LEN = 100;
    const char *paramValues[1];
    

    paramValues[0] = identifierName.c_str(); 
    
    char stm[] = "SELECT id FROM identifiers WHERE name=$1";
    
    PGresult *res = PQexecParams(conn, stm, 1, NULL, paramValues, NULL, NULL, 0);  
    PQsetSingleRowMode(conn);
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {   
        PQclear(res);
        return 0;
    } 
    else
    {
    	if(PQntuples(res) > 0)
    	{
    		char result[LEN];
    		snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));

    		if(result[0] != '\0')
    		{
    			PQclear(res);
    			return atoi(result);
    		}
    		else
    		{
    			PQclear(res);
    			return 0;
    		} 
    	}
    	else
    	{	
    		PQclear(res);
    		return 0;
    	}
   	}
}

long getDomainIdFromDB(PGconn *conn, const string &domainName)
{
 	const int LEN = 100;
    const char *paramValues[1];

    paramValues[0] = domainName.c_str();  
    

    char stm[] = "SELECT id FROM domains WHERE name=$1";
 
    PGresult *res = PQexecParams(conn, stm, 1, NULL, paramValues, NULL, NULL, 0);   
    PQsetSingleRowMode(conn); 
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {   
        PQclear(res);
        return 0;
    } 
    else
    {
    	if(PQntuples(res) > 0)
	    {
	    	char result[LEN];
	    	snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));
	
	    	if(result[0] != '\0')
	    	{
	    		PQclear(res);
	    		return atoi(result);
	    	} 
	   		else
	   		{
	   			PQclear(res);
	   			return 0;
	   		}
	   	}
	   	else
	   	{
	   		PQclear(res);
	   		return 0;
	   	}
    }

}

long getPasswordIdFromDB(PGconn *conn, const string &passwordName)
{
 	const int LEN = 100;
    const char *paramValues[1];
    paramValues[0] = passwordName.c_str();  
    
    char stm[] = "SELECT id FROM passwords WHERE password=$1";
    PGresult *res = PQexecParams(conn, stm, 1, NULL, paramValues, NULL, NULL, 0);   
    PQsetSingleRowMode(conn); 
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {   
        PQclear(res);
        return 0;
    } 
    else
    {
    	if(PQntuples(res) > 0)
	    {
	    	char result[LEN];
	    	snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));
	    	
	    	if(result[0] != '\0')
	    	{
	    		PQclear(res);
	    		return atoi(result);
	    	} 
	   		else
	   		{
	   			PQclear(res);
	   			return 0;
	   		}
	   	}
	   	else
	   	{
	   		PQclear(res);
	   		return 0;
	   	}
    }

}


long insertIdentifierIntoDB (PGconn *conn, const string &identifierName, int IDENTIFIER_TYPE)
{

 	const int LEN = 100;
    const char *paramValues[2];

    paramValues[0] = identifierName.c_str(); 
    
    char idType[2];
    snprintf(idType, 2, "%d", IDENTIFIER_TYPE); 
    paramValues[1] = idType;
    
    char stm[] = "INSERT INTO identifiers (name,type) values ($1,$2) RETURNING id";
    PGresult *res = PQexecParams(conn, stm, 2, NULL, paramValues, NULL, NULL, 0);  
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {   
        PQclear(res);
        return 0;
    }
    else
    {
    	char result[LEN];
    	snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));
    	
    	if(result[0] != '\0')
    	{
    		PQclear(res);
    		//res = PQexec(conn,"COMMIT");
    		//PQclear(res);
    		return atoi(result);
    	} 
   		else
   		{
   			PQclear(res);
   			return 0;
   		}
    }  
}

long insertPasswordIntoDB (PGconn *conn, const string &passwordName)
{
 	const int LEN = 100;
    const char *paramValues[1];
    paramValues[0] = passwordName.c_str(); 
        
    char stm[] = "INSERT INTO passwords (password) values ($1) RETURNING id";
    PGresult *res = PQexecParams(conn, stm, 1, NULL, paramValues, NULL, NULL, 0);  
    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {   
        PQclear(res);
        return 0;
    }
    else
    {
    	char result[LEN];
    	snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));
    	
    	if(result[0] != '\0')
    	{
    		PQclear(res);
    		return atoi(result);
    	} 
   		else
   		{
   			PQclear(res);
   			return 0;
   		}
    }  
}

long insertDomainIntoDB (PGconn *conn, const string &domainName)
{
	const int LEN = 100;
    const char *paramValues[1];
    paramValues[0] = domainName.c_str(); 
        
    // insere e já obtem o id para colocar na cache

    char stm[] = "INSERT INTO domains (name) values ($1) RETURNING id";
    PGresult *res = PQexecParams(conn, stm, 1, NULL, paramValues, NULL, NULL, 0);  

    
    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {   
    	// erro ja existe etc.
        PQclear(res);
        return 0;
    }
    else
    {
    	char result[LEN];
    	snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));
    	
    	if(result[0] != '\0')
    	{
    		PQclear(res);
    		return atoi(result);
    	} 
   		else
   		{
   			// erro vazio fail ja existe etc
   			PQclear(res);
   			return 0;
   		}
    }  
}


		
void updateSourceIntoDB (PGconn *conn, const string &hash, string &dateCompletedAt,  long totalRecords, long sucessRecords, long failedRecords)
{
 	const int LEN = 100;
    const char *paramValues[5];
    
    paramValues[0] = dateCompletedAt.c_str();
    paramValues[1] = hash.c_str(); 

    char totalRecordsBuffer[LEN];
    snprintf(totalRecordsBuffer, LEN, "%lu",totalRecords); 
    paramValues[2] = totalRecordsBuffer;
    
    char sucessRecordsBuffer[LEN];
    snprintf(sucessRecordsBuffer, LEN, "%lu",sucessRecords); 
    paramValues[3] = sucessRecordsBuffer;
    
     char failedRecordsBuffer[LEN];
    snprintf(failedRecordsBuffer, LEN, "%lu",failedRecords); 
    paramValues[4] = failedRecordsBuffer;
    
    char stm[] = "UPDATE sources SET completed_at=$1, total_records=$3, success_records=$4, failed_records=$5 where hash=$2;";
    PGresult *res = PQexecParams(conn, stm, 5, NULL, paramValues, NULL, NULL, 0); 
    
    if (PQresultStatus(res) != PGRES_COMMAND_OK)
    {   
    	// logger?
    	cout << PQresultErrorMessage(res);
    	//cout << "erro postgres" << endl;
        PQclear(res);
    }
    else
    {
    	// erro update db (logger)
    	PQclear(res);
    }
}

long insertSourceIntoDB (PGconn *conn, const string &fileName, const string &hash, const string &dateInitiatedAt)
{
 	const int LEN = 100;
    const char *paramValues[3];
    
   	paramValues[0] = fileName.c_str(); 
    paramValues[1] = hash.c_str(); 
    paramValues[2] = dateInitiatedAt.c_str();

    char stm[] = "INSERT INTO sources (name,hash,initiated_at) values ($1,$2,$3) RETURNING id;";
    PGresult *res = PQexecParams(conn, stm, 3, NULL, paramValues, NULL, NULL, 0); 		
   	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{   
        PQclear(res);
        return 0;
	}
	else
    {
    	char result[LEN];
    	snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));
    	
    	if(result[0] != '\0')
    	{
    		PQclear(res);
    		return atoi(result);
    	} 
   		else
   		{
   			PQclear(res);
   			return 0;
   		}
    }  
}

bool checkIfSourceExistsInDB (PGconn *conn, const string &hash)
{
 	const int LEN = 100;
    const char *paramValues[1];
    
   	paramValues[0] = hash.c_str(); 
  
    char stm[] = "SELECT * from sources where hash = $1;";
    PGresult *res = PQexecParams(conn, stm, 1, NULL, paramValues, NULL, NULL, 0); 		
   	if (PQresultStatus(res) != PGRES_TUPLES_OK)
	{   
        PQclear(res);
        return false;
	}    
	else  
    {
    	if(PQntuples(res) > 0)
	    {
    		char result[LEN];
    		snprintf(result, LEN, "%s", PQgetvalue(res, 0, 0));
    	
	    	if(result[0] != '\0')
	    	{
	    		PQclear(res);
	    		return true;
	    	} 
	   		else
	   		{
	   			PQclear(res);
	   			return false;
	   		}
	   	}
	   	else
	   	{
	   		PQclear(res);
	   		return false;
	   	}
	   	
    }
}


bool commitCopyBatch (PGconn *conn, const string &batch)
{
	const char *errmsg;
    PGresult   *res;
    int a,b;

    errmsg = NULL; 

    res = PQexec(conn,"COPY matches from STDIN DELIMITER '\t' CSV HEADER NULL AS 'n'");
    a = PQputCopyData(conn, batch.c_str(),strlen(batch.c_str()));
    b = PQputCopyEnd(conn, errmsg);

    if (errmsg)
    {
    	printf("Failed:%s\n", errmsg);
    	PQclear(res);
    	return false;
    }
    else
    {
    	PQclear(res);
		return true;    
    }
}


bool commitInsertBatch (PGconn *conn, const string &batch)
{
	const char *errmsg;
	PGresult *res;
    int a,b;

    errmsg = NULL; 

    res = PQexecParams(conn, batch.c_str(), 0, NULL, NULL, NULL, NULL, 0);  

    if (PQresultStatus(res) != PGRES_TUPLES_OK)
    {   
        PQclear(res);
        return true;
    }
    else
    {
    	PQclear(res);
    	return false;
    }
    
}
