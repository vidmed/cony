package cony

import (
	"fmt"
	"math/big"
	"sync"
	"time"
)

type ErrorsBatchSnapshot struct {
	Errors    []error
	ErrorsLen int
	First     *time.Time
	Last      *time.Time
	LastReset *time.Time
}

type ErrorsBatch struct {
	errors    []error
	first     *time.Time
	last      *time.Time
	lastReset *time.Time
	mu        sync.RWMutex
}

func NewErrorBatch() *ErrorsBatch {
	return &ErrorsBatch{}
}

func (eb *ErrorsBatch) Len() int {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return len(eb.errors)
}

func (eb *ErrorsBatch) Errors() []error {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.errors
}

func (eb *ErrorsBatch) First() *time.Time {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.first
}

func (eb *ErrorsBatch) Last() *time.Time {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return eb.last
}

func (eb *ErrorsBatch) Add(err error) {
	now := time.Now()

	eb.mu.Lock()
	defer eb.mu.Unlock()
	if eb.first == nil {
		eb.first = &now
	}
	eb.errors = append(eb.errors, err)
	eb.last = &now
}

func (eb *ErrorsBatch) Reset() {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.first = nil
	eb.last = nil
	eb.errors = nil

	now := time.Now()
	eb.lastReset = &now
}

func (eb *ErrorsBatch) Snapshot() *ErrorsBatchSnapshot {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	return &ErrorsBatchSnapshot{
		Errors:    eb.errors,
		ErrorsLen: len(eb.errors),
		First:     eb.first,
		Last:      eb.last,
		LastReset: eb.lastReset,
	}
}

func (eb *ErrorsBatch) String() string {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	return fmt.Sprintf("ErrorsBatch[First: %s, Last: %s, ErrorsLen: %d, LastReset: %s, Errors: %v]", eb.first, eb.last, len(eb.errors), eb.lastReset, eb.errors)
}

// EPS - errors per second
// CheckEPS takes wanted coefficient and returns if
// current ErrorsBatch eps coefficient grater that given coefficient
func (eb *ErrorsBatch) CheckEPS(k float64) bool {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if eb.last.Unix()-eb.first.Unix() <= 0 {
		return false
	}

	curEPS := new(big.Float).Quo(big.NewFloat(float64(len(eb.errors))), big.NewFloat(float64(eb.last.Unix()-eb.first.Unix()))).SetPrec(5)

	// curEPS >= k
	return curEPS.Cmp(big.NewFloat(k)) >= 0
}

// EPS - errors per second
// GetEPS returns current ErrorsBatch eps coefficient
func (eb *ErrorsBatch) GetEPS() float64 {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	if eb.last.Unix()-eb.first.Unix() <= 0 {
		return 0
	}

	curEPS := new(big.Float).Quo(big.NewFloat(float64(len(eb.errors))), big.NewFloat(float64(eb.last.Unix()-eb.first.Unix()))).SetPrec(5)

	k, _ := curEPS.Float64()
	return k
}
