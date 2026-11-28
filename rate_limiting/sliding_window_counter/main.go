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

func newFixedWindowCounter(
	window_length time.Duration,
	max_requests_per_window int64) *fixedWindowCounter {
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

var allowedRequests, deniedRequests, failedRequests atomic.Uint64

func sendRequest() {
	resp, _ := http.Get("http://localhost:8080")
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		allowedRequests.Add(1)
	case 429:
		deniedRequests.Add(1)
	default:
		failedRequests.Add(1)
	}
}

func main() {
	limiter := newFixedWindowCounter(1 * time.Second, 220)
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
	experimentEnd := time.After(1 * time.Minute)

	// ~250 req/s average with realistic spikes
	meanGapBetweenRequests := 4 * float64(time.Millisecond)
	wg := sync.WaitGroup{}

	for {
		// Simulate Poisson distribution of mean requests per unit time
		poissonRandomSleepTime := time.Duration(rand.ExpFloat64() * meanGapBetweenRequests)

		select {
		case <-experimentEnd:
			wg.Wait()
			fmt.Printf("Allowed Requests: %d\nDeniedRequests: %d\nFailed Requests: %d\n",
				allowedRequests.Load(),
				deniedRequests.Load(),
				failedRequests.Load())
			return
		case <-time.After(poissonRandomSleepTime):
			wg.Go(func() {
				sendRequest()
			})
		}
	}
}
