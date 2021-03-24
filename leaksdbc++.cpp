// Assis @SPI - Parser de vazamentos de credenciais
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_map>
#include <bits/stdc++.h> 
#include <string>
#include <boost/algorithm/string.hpp> 
#include <boost/program_options.hpp>
#include <exception>
#include <postgresql/libpq-fe.h>
#include "leakRecord.h"
#include "DBHandler.h"
#include "manager.h"
#include "lineChunk.h"
#include "pgbackend.h"
#include "pgconnection.h"
#include "ctpl.h"

using namespace std;
namespace po = boost::program_options;

int main(int argc, char **argv)
{
	
	try
	{
		po::options_description desc("Allowed options");
	    desc.add_options()
	        ("help", "Display this help")
	        ("d", po::value<string>()->required(), "path to folder of lists of leaked credentials in plain text format, separated by newline. To end processing, create a file named 'exit' on this folder");

	    po::variables_map vm;
	    po::store(po::parse_command_line(argc, argv, desc), vm);
	    po::notify(vm);

	    if ((vm.count("help")) || (argc == 1))
	    {
	     	cout << desc << endl;
	     	return 0;
	    }
		else
		{
	    	if(vm.count("d"))
		    {
				string leakFilesDir = vm["d"].as<string>();
		    	cout << endl << "Searching for leaked lists in..." << leakFilesDir << endl;
		    	cout << "To end processing, create a file named 'exit on this binary folder." << endl;

		    	manager Manager(leakFilesDir);
	
		    }
		    else
	   	 	{
	    		cout << desc << endl;
	    	}
		 }
	}
	catch(exception& e)
	{
		cerr << "error: " << e.what() << endl;
	}
	catch(...)
	{
		cerr << "Unknown exception type" << endl;
	}	
	return 0;	
}
