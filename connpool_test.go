package connpool

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestPool_Put(t *testing.T) {
	count := 0
	p, err := New(&Config{
		Initial:     10,
		Max:         100,
		MaxIdle:     30,
		IdleTimeout: time.Second * 60,
		FactoryFunc: func() (interface{}, error) {
			count++
			return 1, nil
		},
		CloseFunc: func(i interface{}) error {
			count--
			return nil
		},
	})
	if err != nil {
		t.Fatalf("error creating pool: %v", err)
	}
	wg := sync.WaitGroup{}
	var rerr error
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			if conn, err := p.Get(); err != nil {
				rerr = fmt.Errorf("error get connection: %w", err)
			} else {
				time.Sleep(time.Millisecond * time.Duration(rand.Int()%100))
				if err := p.Put(conn, false); err != nil {
					rerr = err
				}
			}
		}()

	}
	wg.Wait()
	if rerr != nil {
		log.Fatal(rerr)
	}
	if count > 30 {
		t.Fatalf("invalid connection count: %d", count)
	}
	p.Close()
}

func TestPool_ReleaseIdle(t *testing.T) {
	count := 0
	p, err := New(&Config{
		Initial:     3,
		Max:         5,
		MaxIdle:     3,
		IdleTimeout: time.Second * 60,
		FactoryFunc: func() (interface{}, error) {
			count++
			return 1, nil
		},
		CloseFunc: func(i interface{}) error {
			count--
			return nil
		},
	})
	if err != nil {
		t.Fatalf("error creating pool: %v", err)
	}
	p.ReleaseIdle()
	if count != 0 {
		t.Fatalf("invalid connection count: %d", count)
	}
	p.Close()
}

func TestPool_Len(t *testing.T) {
	count := 0
	p, err := New(&Config{
		Initial:     3,
		Max:         5,
		MaxIdle:     3,
		IdleTimeout: time.Second * 60,
		FactoryFunc: func() (interface{}, error) {
			count++
			return 1, nil
		},
		CloseFunc: func(i interface{}) error {
			count--
			return nil
		},
	})
	if err != nil {
		t.Fatalf("error creating pool: %v", err)
	}
	l := p.Len()
	if l != count {
		t.Fatalf("invalid length: %d != %d", l, count)
	}
	for i := 0; i < 5; i++ {
		_, err := p.Get()
		if err != nil {
			log.Fatalf("error get connection: %v", err)
		}
	}
	l = p.Len()
	if l != count {
		t.Fatalf("invalid length: %d != %d", l, count)
	}
}

func TestPool_Close(t *testing.T) {
	p, err := New(&Config{
		Initial:     3,
		Max:         5,
		MaxIdle:     3,
		IdleTimeout: time.Second * 60,
		FactoryFunc: func() (interface{}, error) {
			return 1, nil
		},
		CloseFunc: func(i interface{}) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("error creating pool: %v", err)
	}
	wg := sync.WaitGroup{}
	for i := 0; i < 6; i++ {
		if i == 5 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(time.Millisecond * 100)
				p.Close()
			}()
		}
		_, err := p.Get()
		if err != nil {
			if !errors.Is(err, ErrPoolClosed) {
				log.Fatalf("error get connection: %v", err)
			}
		}
	}
	wg.Wait()
}
