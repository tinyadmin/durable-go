package cursor

import (
	"math/rand"
	"strconv"
	"time"
)

// Default epoch for cursor calculation (October 9, 2024 00:00:00 UTC).
var Epoch = time.Date(2024, 10, 9, 0, 0, 0, 0, time.UTC)

// DefaultInterval is the default cursor interval.
const DefaultInterval = 20 * time.Second

// Generator generates CDN cache collapsing cursors.
type Generator struct {
	epoch    time.Time
	interval time.Duration
	rng      *rand.Rand
}

// New creates a new cursor generator.
func New() *Generator {
	return &Generator{
		epoch:    Epoch,
		interval: DefaultInterval,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewWithConfig creates a cursor generator with custom config.
func NewWithConfig(epoch time.Time, interval time.Duration) *Generator {
	return &Generator{
		epoch:    epoch,
		interval: interval,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Current returns the current cursor value.
func (g *Generator) Current() string {
	elapsed := time.Since(g.epoch)
	intervalNum := int64(elapsed / g.interval)
	return strconv.FormatInt(intervalNum, 10)
}

// Next returns a cursor strictly greater than the provided one.
// This prevents cache loops when client cursor >= current interval.
func (g *Generator) Next(clientCursor string) string {
	current := g.Current()

	if clientCursor == "" {
		return current
	}

	clientVal, err := strconv.ParseInt(clientCursor, 10, 64)
	if err != nil {
		return current
	}

	currentVal, _ := strconv.ParseInt(current, 10, 64)

	if clientVal >= currentVal {
		// Add jitter (1-3600 seconds, converted to intervals)
		// Minimum 1 interval to ensure strictly greater value
		jitterSeconds := 1 + g.rng.Intn(3600)
		jitterIntervals := int64(jitterSeconds) / int64(g.interval/time.Second)
		if jitterIntervals < 1 {
			jitterIntervals = 1
		}
		newVal := clientVal + jitterIntervals
		return strconv.FormatInt(newVal, 10)
	}

	return current
}
