package connpool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

var (
	ErrPoolClosed = fmt.Errorf("pool closed")
	ErrPoolFull   = fmt.Errorf("pool is full. put without get ?")
)

type Config struct {
	// initial connections count
	Initial int
	// max connections count
	Max int
	// max idle connections count
	MaxIdle int
	// idle connection timeout
	IdleTimeout time.Duration
	// timeout precision
	TimeoutPrecision time.Duration
	// factory function
	FactoryFunc func() (interface{}, error)
	// check idle function
	CheckIdleFunc func(interface{}) bool
	// close function
	CloseFunc func(interface{}) error
}

type idleConnWrap struct {
	conn         interface{}
	lastActiveAt time.Time
}

type Pool struct {
	wg               sync.WaitGroup
	workerMutex      sync.Mutex
	done             chan struct{}
	idleConns        chan idleConnWrap
	slots            chan struct{}
	factoryFunc      func() (interface{}, error)
	closeFunc        func(interface{}) error
	checkIdleFunc    func(interface{}) bool
	max              int
	maxIdle          int
	idleTimeout      time.Duration
	timeoutPrecision time.Duration
}

// Create a new connection pool
func New(config *Config) (*Pool, error) {
	if config == nil {
		return nil, fmt.Errorf("no config given")
	}
	if config.FactoryFunc == nil {
		return nil, fmt.Errorf("factory function must be defined")
	}
	if config.CloseFunc == nil {
		return nil, fmt.Errorf("close function must be defined")
	}
	if config.Initial > config.MaxIdle || config.Initial > config.Max || config.MaxIdle > config.Max {
		return nil, fmt.Errorf("invalid configuration")
	}
	p := Pool{
		done:          make(chan struct{}),
		idleConns:     make(chan idleConnWrap, config.MaxIdle),
		slots:         make(chan struct{}, config.Max),
		factoryFunc:   config.FactoryFunc,
		checkIdleFunc: config.CheckIdleFunc,
		closeFunc:     config.CloseFunc,
		max:           config.Max,
		maxIdle:       config.MaxIdle,
		idleTimeout:   config.IdleTimeout,
	}
	if config.TimeoutPrecision == 0 {
		p.timeoutPrecision = time.Second * 10
	} else {
		p.timeoutPrecision = config.TimeoutPrecision
	}
	for i := 0; i < p.max; i++ {
		p.slots <- struct{}{}
	}
	for i := 0; i < config.Initial; i++ {
		<-p.slots
		if conn, err := p.factoryFunc(); err != nil {
			p.ReleaseIdle()
			return nil, fmt.Errorf("can't create initial connection pool: %w", err)
		} else {
			p.idleConns <- idleConnWrap{
				conn:         conn,
				lastActiveAt: time.Now(),
			}
		}
	}
	p.startWorker()
	return &p, nil
}

// Gets a connection from pool.
// If no idle connections available - creates new one.
// Function blocks until idle connection not available or connections count greater or equal max connections
func (p *Pool) Get() (interface{}, error) {
	return p.GetWithContext(context.Background())
}

// Gets a connection from pool with context
func (p *Pool) GetWithContext(ctx context.Context) (interface{}, error) {
	p.workerMutex.Lock()
	defer p.workerMutex.Unlock()

	p.wg.Add(1)
	defer p.wg.Done()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.done:
		return nil, ErrPoolClosed
	default:
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-p.done:
			return nil, ErrPoolClosed
		case c := <-p.idleConns:
			return c.conn, nil
		default:
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-p.done:
				return nil, ErrPoolClosed
			case c := <-p.idleConns:
				return c.conn, nil
			case <-p.slots:
				if conn, err := p.factoryFunc(); err != nil {
					p.slots <- struct{}{}
					return nil, err
				} else {
					return conn, nil
				}
			}
		}
	}
}

// Puts connections to pool
func (p *Pool) Put(conn interface{}, close bool) error {
	p.wg.Add(1)
	defer p.wg.Done()

	t := time.Time{}
	if !close {
		t = time.Now()
	}

	return p.putConn(conn, t)
}

func (p *Pool) putConn(conn interface{}, lastActiveAt time.Time) error {
	select {
	case <-p.done:
		return ErrPoolClosed
	default:
	}

	if !lastActiveAt.IsZero() && lastActiveAt.After(time.Now().Add(-p.idleTimeout)) {
		select {
		case <-p.done:
			return ErrPoolClosed
		case p.idleConns <- idleConnWrap{conn: conn, lastActiveAt: lastActiveAt}:
			return nil
		default:
		}
	}

	select {
	case <-p.done:
		return ErrPoolClosed
	case p.slots <- struct{}{}:
		return p.closeFunc(conn)
	default:
		return ErrPoolFull
	}
}

func (p *Pool) startWorker() {
	go func() {
		p.wg.Add(1)
		defer p.wg.Done()

		for {
			select {
			case <-p.done:
				return
			case <-time.After(p.timeoutPrecision):
				checkIdle := func() {
					p.workerMutex.Lock()
					defer p.workerMutex.Unlock()

					idleConns := make(chan idleConnWrap, p.maxIdle)
				loop:
					for {
						select {
						case c := <-p.idleConns:
							idleConns <- c
						default:
							break loop
						}
					}
					close(idleConns)
					now := time.Now()
					for c := range idleConns {
						t := c.lastActiveAt
						if p.checkIdleFunc != nil {
							if !p.checkIdleFunc(c.conn) {
								t = now
							}
						}
						_ = p.putConn(c.conn, t)
					}
				}
				checkIdle()
			}
		}
	}()
}

// current number of connections
func (p *Pool) Len() int {
	return p.max - len(p.slots)
}

// Closes all idle connections
func (p *Pool) ReleaseIdle() {
loop:
	for {
		select {
		case c := <-p.idleConns:
			_ = p.closeFunc(c.conn)
			p.slots <- struct{}{}
		default:
			break loop
		}
	}
}

// Releases pool. After close complete pool is not usable
func (p *Pool) Close() {
	close(p.done)
	p.wg.Wait()
	p.ReleaseIdle()
	close(p.idleConns)
	close(p.slots)
	for _ = range p.slots {
	}
}
