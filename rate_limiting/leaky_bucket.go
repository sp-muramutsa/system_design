package main

import (
    "fmt"
    "math/rand/v2"
	"net/http"
	"sync"
	"time"
)

type leakyBucket struct {
    mu              sync.Mutex
    currCapacity   int64
    maxCapacity    int64
    refillRate     int64
}

func newLeakyBucket(maxCapacity int64, currCapacity int64, rate int64) *leakyBucket {
    bucket := leakyBucket{
        maxCapacity:   maxCapacity, 
        currCapacity:  currCapacity,
        refillRate:    rate,
    }
    return &bucket
}


func (b *leakyBucket) useToken() (bool, int64) {
    b.mu.Lock()
    defer b.mu.Unlock()
    
    if b.currCapacity <= 0 {
        return false, 0
    } 
    
    b.currCapacity = max(b.currCapacity - 1, 0)
    return true, b.currCapacity
}

func (b *leakyBucket) refillBucket() {
    b.mu.Lock()
    defer b.mu.Unlock()
    candidate := b.currCapacity + b.refillRate 
    b.currCapacity = min(candidate, b.maxCapacity)
}

type customServer struct {
    rateLimiter *leakyBucket
    httpServer  *http.Server
} 

func newServer(addr string, limiter *leakyBucket) *customServer {
    s := customServer{
        rateLimiter: limiter,
        httpServer:  &http.Server{Addr: addr},
    }
    s.httpServer.Handler = http.HandlerFunc(s.handleRequest)
    return &s
}

func (s *customServer) handleRequest(w http.ResponseWriter, r *http.Request) {

    canHandleRequest, remainingTokens := s.rateLimiter.useToken()
    
    if !canHandleRequest {
        w.Header().Set("Retry-After", "15")
        w.WriteHeader(http.StatusTooManyRequests)
        fmt.Printf("Request Couldn't be handled. No tokens available.\n")
        return
    } 
    w.WriteHeader(http.StatusOK)
    fmt.Printf("Request handled successfully. %d Remaining tokens.\n", remainingTokens)
}

func main() {
    
    rateLimiter := newLeakyBucket(10000, 10000, 2000)
    
    s := newServer(":8080", rateLimiter)
    go s.httpServer.ListenAndServe()
    
    refillTicker := time.NewTicker(10 * time.Second)
    defer refillTicker.Stop()

    done := make(chan bool)

    go func() {
        for {
            select {
            case <-done:
                return
            case <-refillTicker.C:
                rateLimiter.refillBucket()
            }
        }
    }()

    // Simulate requests
    experimentEnd := time.After(1 * time.Minute) 
    requestTicker := time.NewTicker(2 * time.Millisecond)

    var wg sync.WaitGroup
    for {
        select {
        case <-experimentEnd:
            wg.Wait()
            close(done)
            return
        case <-requestTicker.C:
            wg.Add(1)
            go func() {
                defer wg.Done()
                resp, err := http.Get("http://localhost:8080")
                if err != nil {
                    return
                }
                resp.Body.Close()
            }()
            // Randomly sleep for a few milliseconds to simulate randomness in the requests (Poisson Distribution)
            meanGap := 4 * time.Millisecond
            sleepDuration := time.Duration(rand.ExpFloat64() * float64(meanGap))
            time.Sleep(sleepDuration)
        }
    }
}

