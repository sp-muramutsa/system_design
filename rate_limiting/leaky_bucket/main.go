package main

import (
    "fmt"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type leakyBucket struct {
	queue    chan queuedRequest
	ticker   *time.Ticker
	done     chan struct{}
	leakRate int
}

func newLeakyBucket(leakFrequency time.Duration, bucketSize int, leakRate int) *leakyBucket {
	b := leakyBucket{
		queue:    make(chan queuedRequest, bucketSize),
		ticker:   time.NewTicker(leakFrequency),
		done:     make(chan struct{}),
		leakRate: leakRate,
	}
	return &b
}

func (b *leakyBucket) leakRequest(server *customServer) {
	defer b.ticker.Stop()

	for {
		select {
		case <-b.done:
			return
		case <-b.ticker.C:
			availableRequests := len(b.queue)
			for range min(b.leakRate, availableRequests) {
				req := <-b.queue
				server.closeRequest(req)
			}
		}
	}
}

type queuedRequest struct {
	ready chan struct{}
}

type customServer struct {
	server        http.Server
	rateLimiter   *leakyBucket
	successCount  atomic.Int64
	rejectedCount atomic.Int64
}

func newCustomServer(limiter *leakyBucket) *customServer {
	s := customServer{
		server:      http.Server{Addr: ":8080"},
		rateLimiter: limiter,
	}
	s.server.Handler = http.HandlerFunc(s.handleRequest)
	return &s
}

func (s *customServer) handleRequest(w http.ResponseWriter, r *http.Request) {
	pendingRequest := queuedRequest{ready: make(chan struct{})}

	select {
	case <-s.rateLimiter.done:
		w.WriteHeader(http.StatusServiceUnavailable)
	case s.rateLimiter.queue <- pendingRequest:
		select {
		case <-pendingRequest.ready:
			w.WriteHeader(http.StatusOK)
			s.successCount.Add(1)
		case <-s.rateLimiter.done:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	default:
		w.WriteHeader(http.StatusTooManyRequests)
		s.rejectedCount.Add(1)
	}
}

func (s *customServer) closeRequest(r queuedRequest) {
	close(r.ready)
}

func main() {
	rateLimiter := newLeakyBucket(1*time.Second, 500, 200) // leaks 200 req/s
	s := newCustomServer(rateLimiter)
	go s.server.ListenAndServe()
	go rateLimiter.leakRequest(s)

	// wait group of some client
	wg := sync.WaitGroup{}

	// Simulate traffic
	experimentEnd := time.After(1 * time.Minute)
	meanGap := 4 * time.Millisecond // ~250 req/s average with realistic bursts

	for {
		sleepDuration := time.Duration(rand.ExpFloat64() * float64(meanGap))
		select {
		case <-experimentEnd:
			close(rateLimiter.done)
			wg.Wait()
			fmt.Printf("\nResults:\nHTTP 200 (OK): %d\nHTTP 429 (Too Many Requests): %d\n",
				s.successCount.Load(), s.rejectedCount.Load())
			return
		case <-time.After(sleepDuration):
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Get("http://localhost:8080")
				if err != nil {
					return
				}
				resp.Body.Close()
			}()
		}
	}
}
