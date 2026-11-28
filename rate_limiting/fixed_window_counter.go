package main

import (
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
