CXX = g++-8
CXXFLAGS = -O3 -std=c++17 
INCLUDE = -I /usr/include/postgresql -I /usr/include/boost/
LDFLAGS = -L/usr/lib/x86_64-linux-gnu -lpq -lssl -lcrypto -lboost_system -lboost_filesystem -lpthread -lboost_program_options
default: leaksdbc++

leaksdbc++: leaksdbc++.o 
	$(CXX) $(CFLAGS) $(INCLUDE) -o leaksdbc++ leaksdbc++.o $(LDFLAGS)

leaksdbc++.o: leaksdbc++.cpp fileProcessor.h leakRecord.h manager.h leakFile.h batch.h lineChunk.h lineChunkProcessor.h robin_hood.h pgbackend.h pgconnection.h

clean: $(RM) leaksdbc++ *.o *~
