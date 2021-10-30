# connpool
Connection pool for golang

### USAGE

1. Initialize new pool:

```go
// create a connection pool:
// initial connections: 2
// maximum connections: 10
// maximum idle connections: 2
// idle timeout: 2 min (after this time idle connection closed)
pool, err := connpool.New(&connpool.Config{
	Initial: 2,
	Max: 10,
	MaxIdle: 2,
	IdleTimeout: time.Minute * 2,
	FactoryFunc: func() (interface{}, error) {
        return net.Dial("tcp", "127.0.0.1:1234")	
    },
	CloseFunc: func(conn interface{}) error {
        return v.(net.Conn).Close()
    }
})

// get connection from pool
conn, err := pool.Get()

// ...
// do anything with conn (but don't close)

// put connection to pool
err := pool.Put(conn)

// get number of opened connections
l := pool.Len()

// close pool if not needed:
pool.Close()


```
