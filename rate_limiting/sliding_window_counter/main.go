package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type fixedWindowCounter struct {
	max_requests_per_window int64
	requests_this_window    atomic.Int64
	ticker                  *time.Ticker
	done                    chan struct{}
}

func newFixedWindowCounter(window_length time.Duration, max_requests_per_window int64) *fixedWindowCounter {
	return &fixedWindowCounter{
		max_requests_per_window: max_requests_per_window,
		ticker:                  time.NewTicker(window_length),
		done:                    make(chan struct{}),
	}
}

func (c *fixedWindowCounter) canHandleRequest() bool {
	curr_requests_this_window := c.requests_this_window.Add(1)
	if curr_requests_this_window > c.max_requests_per_window {
		return false
	}
	return true
}

func (c *fixedWindowCounter) resetWindowCounter() {
	defer c.ticker.Stop()
	for {
		select {
		case <-c.ticker.C:
			c.requests_this_window.Store(0)
		case <-c.done:
			return
		}
	}
}

func main() {
	limiter := newFixedWindowCounter(1*time.Second, 220)
	go limiter.resetWindowCounter()
	defer close(limiter.done)

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !limiter.canHandleRequest() {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	s := http.Server{Addr: ":8080", Handler: mux}
	go s.ListenAndServe()

	// Simulate Traffic
	wg := sync.WaitGroup{}
	meanGapBetweenRequests := 4.0 * float64(time.Millisecond) // ~250 req/s average with realistic bursts

	experimentEnd := time.After(1 * time.Minute)
	var allowedRequestsCount, deniedRequestsCount atomic.Uint64

	for {
		// Simulate Poisson distribution of mean requests per unit time
		randomSleepTime := time.Duration(rand.ExpFloat64() * meanGapBetweenRequests)
		select {
		case <-experimentEnd:
			wg.Wait()
			fmt.Printf("Accepted Requests Count: %d, Denied Requests Count: %d\n", allowedRequestsCount.Load(), deniedRequestsCount.Load())
			return
		case <-time.After(randomSleepTime):
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Get("http://localhost:8080")
				if err != nil {
					return
				}
				defer resp.Body.Close()

				switch resp.StatusCode {
				case 429:
					deniedRequestsCount.Add(1)
				case 200:
					allowedRequestsCount.Add(1)
				default:
					deniedRequestsCount.Add(1)
					fmt.Printf("Unkown error happened")
				}
			}()
		}
	}
}
