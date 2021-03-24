#include <iostream>
#include <string>
#define TIPO_INVALIDO 0
#define TIPO_CPF 1
#define TIPO_CNPJ 2
#define TIPO_EMAIL 3
#define TIPO_PHONE 4
#define TIPO_CEP 5
#define TIPO_NIT 6
#define TIPO_PIS 7
#define TIPO_USERNAME 8
using namespace std;

#ifndef LEAK_RECORD_H_INCLUDED
#define LEAK_RECORD_H_INCLUDED

class leakRecord
{
	
	public:

	string identifierName;
	string domainName;
	string passwordName;
	long identifierID;
	long domainID;
	long passwordID;
	
	int identifierType = TIPO_INVALIDO;
	
	leakRecord ()
	{
		this->identifierName.reserve(100);
		this->domainName.reserve(100);
		this->passwordName.reserve(100);
		this->identifierID = 0;
		this->domainID = 0;
		this->passwordID = 0;
		
	}
	
	void buildLeakRecord(const string &identifierName, const string &domainName, const string &passwordName)
	{
		this->identifierName = identifierName;
		this->domainName = domainName;
		this->passwordName = passwordName;
	}
	
	void buildLeakRecord(const string &identifierName, const string &passwordName)
	{
		this->identifierName = identifierName;
		this->domainName = "";
		this->passwordName = passwordName;
	}
	
	void printRecord()
	{
		cout << this->identifierName << this->domainName << this->passwordName << endl;
	}

	
	void format ()
	{
		
	}
	
	inline bool isCPF()
	{
		const char *buffer;
		buffer = this->identifierName.c_str();
		int tam = strlen(buffer);
			
		if(buffer!= NULL)
		{
		
			int digito1, digito2, temp = 0;
	
		    for(char i = 0; i < 9; i++)
		    {
		    	temp += (buffer[i] - '0') * (10 - i);
		    }
		      
		    temp %= 11;
		
		    if(temp < 2)
		    {
		    	digito1 = 0;
		    }
		       
		    else
		    {
		    	digito1 = 11 - temp;
		    }
		        
		    temp = 0;
		    
		    for(char i = 0; i < 10; i++)
		    {
		    	temp += (buffer[i] - '0') * (11 - i);
		    }
		       
		    temp %= 11;
		
		    if(temp < 2)
		    {
		    	digito2 = 0;
		    }
		    else
		    {
		    	digito2 = 11 - temp;
		    }
		       	
		    if((digito1 == buffer[9] - '0') && digito2 == buffer[10] - '0')
		    {
		    	return true;
		    }
		    else
		    {
		    	return false;
		    }
		}
		else
		{
			return false;
		}
	}
	
	

	inline bool isCNPJ()
	{
			const char *buffer;
			buffer = this->identifierName.c_str();
			int tam = strlen(buffer);
			
			if(tam < 14)
			{
				return false;
			}
			
			else
			{
		    	int soma1 = 0;
		    	int soma2 = 0;
		    	int mult[14] = {6,5,4,3,2,9,8,7,6,5,4,3,2};
		    	int i=0;
		    	int digit1,digit2 = 0;
		    	
		    	for(i=0;i<12;i++)
				{
					soma1 = soma1 + ((buffer[i] - '0') * mult[i+1]);
				}
				digit1 = 11 - (int) (soma1 % 11);
		    		
	    		if(digit1 == 11 || digit1 == 10)
	    		{
	    			digit1 = 0;
	    		}
	
				if(digit1 != (buffer[12] -'0'))
				{
					return false;
				}
				else
				{
					for(i=0;i<13;i++)
		    		{
		    			soma2 = soma2 + ((buffer[i] - '0') * mult[i]);
		    		}
		    		digit2 = 11 - (int) (soma2 % 11);
		    		
		    		if(digit2 == 11 || digit2 == 10)
		    		{
		    			digit2 = 0;
		    		}
		    		
		    		if(digit2 != (buffer[13] -'0'))
		    		{
		    			return false;
		    		}
		    		else
		    		{
		    			return true;
		    		}
		    		
				}
		}
	}
	
	inline bool validate()
	{

		int identifierNameSize = 0;
		int domainNameSize = 0;
		int passwordNameSize = 0;
		
		identifierNameSize = this->identifierName.length();
		domainNameSize = this->domainName.length();
		passwordNameSize = this->passwordName.length();
		
		if((identifierNameSize > 0) && (passwordNameSize > 0) && (identifierNameSize < 120) && (passwordNameSize < 120))
		{
			if((domainNameSize > 0) && (domainNameSize < 120)) 
			{
				this->identifierType = TIPO_EMAIL;
			}
			else
			{
				if(isCPF())
				{
					this->identifierType = TIPO_CPF;
				}
				else
				{
					if(isCNPJ())
					{
						this->identifierType = TIPO_CNPJ;
					}
					else
					{
						this->identifierType = TIPO_USERNAME;
					}
				}
			}
			return true;
		}
		else
		{
			return false;
		}
	}
};

#endif
