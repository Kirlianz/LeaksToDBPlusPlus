#include <iostream>
#include <fstream>
#include <string>
#include <openssl/md5.h>
#include <time.h>
#include <sys/time.h>
#include <sstream>
#define TIPO_INVALIDO 0
using namespace std;



class leakFile
{
	public:
		string fileName;
		string filePath;
		string hint;
		string MD5hash;
		string dateInitiatedAt;
		string dateCompletedAt;
		uintmax_t size;
		long totalRecords;
		long sucessRecords;
		long failedRecords;
		
		
		int getMD5Hash()
		{
			unsigned char c[MD5_DIGEST_LENGTH];
		    int i;
		    FILE *inFile = fopen (&filePath[0], "rb");
		    MD5_CTX mdContext;
		    int bytes;
		    unsigned char data[1024];
		
		    if (inFile == NULL)
		    {
		        printf ("%s can't be opened.\n", &fileName[0]);
		        return 0;
		    }
		    MD5_Init (&mdContext);
		    while ((bytes = fread (data, 1, 1024, inFile)) != 0)
		    {
		    	MD5_Update (&mdContext, data, bytes);
		    }
		    MD5_Final (c,&mdContext);
		    char buf[33];

		  	for (i = 0; i < 16; i++)
		  	{
          		sprintf(&buf[i*2],"%02x", (unsigned int)c[i]);
       		}
       		MD5hash = buf;

		    fclose (inFile);
		    return 0;
		}

		void getDateInitiatedAt ()
		{
			//Format 2020-10-31 18:18:30
			time_t rawtime;
		  	struct tm *info;
		   	char buffer[80];
		   	time( &rawtime );
		   	info = localtime( &rawtime );
			strftime(buffer,80,"%Y-%m-%d %H:%M:%S", info);
			dateInitiatedAt = buffer;
		}	
		
		void getDateCompletedAt ()
		{
			//Format 2020-10-31 18:18:30
			time_t rawtime;
		  	struct tm *info;
		   	char buffer[80];
		   	time( &rawtime );
		   	info = localtime( &rawtime );
			strftime(buffer,80,"%Y-%m-%d %H:%M:%S", info);
			dateCompletedAt = buffer;
		}
};


