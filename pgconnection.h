#ifndef PGCONNECTION_H
#define PGCONNECTION_H

#include <memory>
#include <mutex>
#include <postgresql/libpq-fe.h>


class PGConnection
{
	public:
	    PGConnection()
		{
		    m_connection.reset( PQsetdbLogin(m_dbhost.c_str(), std::to_string(m_dbport).c_str(), nullptr, nullptr, m_dbname.c_str(), m_dbuser.c_str(), m_dbpass.c_str()), &PQfinish );
		
		    if (PQstatus( m_connection.get() ) != CONNECTION_OK && PQsetnonblocking(m_connection.get(), 1) != 0 )
		    {
		       throw std::runtime_error( PQerrorMessage( m_connection.get() ) );
		    }
		
		}
	
	    std::shared_ptr<PGconn> connection() const
		{
		    return m_connection;
		}
			
	private:
	    void establish_connection();
	
		// conn info goes here
	    std::string m_dbhost = "";
	    int         m_dbport = 5432;
	    std::string m_dbname = "";
	    std::string m_dbuser = "";
	    std::string m_dbpass = "";
	    std::shared_ptr<PGconn>  m_connection;
};


#endif //PGCONNECTION_H
