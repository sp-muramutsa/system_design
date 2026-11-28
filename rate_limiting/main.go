package main

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type requestLimiter interface {
	canHandleRequest() bool
}

func main() {
	fmt.Println("Choose a rate-limiting algorithm:")
	fmt.Println("1. Fixed window counter")
	fmt.Println("2. Sliding window log")
	fmt.Print("Enter 1 or 2: ")

	var limiter requestLimiter
	var closeLimiter func()
	var choice int
	if _, err := fmt.Scanln(&choice); err != nil {
		fmt.Printf("Read choice: %v\n", err)
		return
	}
	switch choice {
	case 1:
		counter := newFixedWindowCounter(time.Second, 220)
		go counter.resetWindowCounter()
		limiter = counter
		closeLimiter = func() { close(counter.done) }
	case 2:
		limiter = newSlidingWindowLog(1000, 5*time.Second)
	default:
		fmt.Printf("Unknown choice: %d\n", choice)
		return
	}
	if closeLimiter != nil {
		defer closeLimiter()
	}

	var allowed, denied, failed atomic.Uint64
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !limiter.canHandleRequest() {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	server := &http.Server{Addr: ":8080", Handler: mux}
	go server.ListenAndServe()
	defer server.Close()

	var requests sync.WaitGroup

	// Simulate Traffic
	experimentEnd := time.After(time.Minute)

	// ~250 req/s average with realistic spikes
	meanGap := 4 * time.Millisecond

	for {
		// Simulate Poisson distribution of mean requests per unit time
		sleep := time.Duration(rand.ExpFloat64() * float64(meanGap))
		select {
		case <-experimentEnd:
			requests.Wait()
			fmt.Printf("Allowed Requests: %d\nDenied Requests: %d\nFailed Requests: %d\n",
				allowed.Load(), denied.Load(), failed.Load())
			return
		case <-time.After(sleep):
			requests.Go(func() {
				resp, err := http.Get("http://localhost:8080")
				if err != nil {
					failed.Add(1)
					return
				}
				defer resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusOK:
					allowed.Add(1)
				case http.StatusTooManyRequests:
					denied.Add(1)
				default:
					failed.Add(1)
				}
			})
		}
	}
}
