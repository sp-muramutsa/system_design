package main

import (
	"errors"
	"sync"
	"time"
)

type ringBuffer struct {
	array                         []time.Time
	head, tail, currSize, maxSize int
}

func newRingBuffer(maxSize int) *ringBuffer {
	return &ringBuffer{
		array:   make([]time.Time, maxSize),
		maxSize: maxSize,
	}
}

func (r *ringBuffer) isEmpty() bool {
	return r.currSize == 0
}

func (r *ringBuffer) isFull() bool {
	return r.currSize >= len(r.array)
}

func (r *ringBuffer) push(timestamp time.Time) error {
	if r.isFull() {
		return errors.New("Push failed. Can't push to a full buffer.")
	}
	r.array[r.tail] = timestamp
	r.tail = (r.tail + 1) % r.maxSize
	r.currSize++
	return nil
}

func (r *ringBuffer) pop() error {
	if r.isEmpty() {
		return errors.New("Pop failed. Can't pop from an empty array.")
	}
	r.head = (r.head + 1) % r.maxSize
	r.currSize--
	return nil
}

func (r *ringBuffer) peek() (time.Time, error) {
	if r.isEmpty() {
		return time.Time{}, errors.New("Pop failed. Can't pop from an empty array.")
	}
	return r.array[r.head], nil
}

type slidingWindowLog struct {
	queue        ringBuffer
	windowLength time.Duration
	mu           sync.Mutex
}

func newSlidingWindowLog(maxLogSize int, windowSize time.Duration) *slidingWindowLog {
	return &slidingWindowLog{
		queue:        *newRingBuffer(maxLogSize),
		windowLength: windowSize,
	}
}

func (l *slidingWindowLog) canHandleRequest() bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	startOfCurrentWindow := now.Add(-l.windowLength)
	l.evictAllOlderThan(startOfCurrentWindow)

	if l.queue.isFull() {
		return false
	}

	l.queue.push(now)
	return true
}

func (l *slidingWindowLog) evictAllOlderThan(target time.Time) {
	for {
		timestamp, err := l.queue.peek()
		if err != nil || !timestamp.Before(target) {
			break
		}
		l.queue.pop()
	}
}
