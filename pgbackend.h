#ifndef PGBACKEND_H
#define PGBACKEND_H
#include <memory>
#include <mutex>
#include <string>
#include <queue>
#include <condition_variable>
#include <postgresql/libpq-fe.h>
#include "pgconnection.h"

class PGBackend
{
public:

    PGBackend()
	{
	    createPool();  
	
	}
	
    std::shared_ptr<PGConnection> connection()
	{
	
	    std::unique_lock<std::mutex> lock_( m_mutex );
	
	    while ( m_pool.empty() ){
	            m_condition.wait( lock_ );
	    }
	
	    auto conn_ = m_pool.front();
	    m_pool.pop();
	
	    return  conn_;
	}
	    
    void freeConnection(std::shared_ptr<PGConnection> conn_)
	{
	    std::unique_lock<std::mutex> lock_( m_mutex );
	    m_pool.push( conn_ );
	    lock_.unlock();
	    m_condition.notify_one();
	}
	    
    
    

private:
    
    
    std::mutex m_mutex;
    std::condition_variable m_condition;
    std::queue<std::shared_ptr<PGConnection>> m_pool;

    const int POOL = 11;
    
    void createPool()
	{
	    std::lock_guard<std::mutex> locker_( m_mutex );
	
	    for ( auto i = 0; i< POOL; ++i ){
	         m_pool.emplace ( std::make_shared<PGConnection>() );
	    }
	}


};

#endif //PGBACKEND_H
