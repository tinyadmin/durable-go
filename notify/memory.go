package notify

import (
	"sync"

	"github.com/me/durable/storage"
)

// MemoryNotifier is an in-memory implementation of Notifier.
type MemoryNotifier struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan storage.Notification]string // url -> (chan -> offset)
	closed      bool
}

// NewMemory creates a new in-memory notifier.
func NewMemory() *MemoryNotifier {
	return &MemoryNotifier{
		subscribers: make(map[string]map[chan storage.Notification]string),
	}
}

// Subscribe registers for notifications on a stream.
func (n *MemoryNotifier) Subscribe(url string, offset string) <-chan storage.Notification {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		ch := make(chan storage.Notification, 1)
		close(ch)
		return ch
	}

	ch := make(chan storage.Notification, 1)

	if n.subscribers[url] == nil {
		n.subscribers[url] = make(map[chan storage.Notification]string)
	}
	n.subscribers[url][ch] = offset

	return ch
}

// Unsubscribe removes a notification subscription.
func (n *MemoryNotifier) Unsubscribe(url string, ch <-chan storage.Notification) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if subs, ok := n.subscribers[url]; ok {
		// Find the matching channel
		for subCh := range subs {
			if subCh == ch {
				delete(subs, subCh)
				close(subCh)
				break
			}
		}
		if len(subs) == 0 {
			delete(n.subscribers, url)
		}
	}
}

// Notify sends a notification to all subscribers of a stream.
func (n *MemoryNotifier) Notify(url string, offset string) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.closed {
		return
	}

	subs, ok := n.subscribers[url]
	if !ok {
		return
	}

	for ch, subscribedOffset := range subs {
		// Only notify if new offset is after subscribed offset
		if offset > subscribedOffset {
			select {
			case ch <- storage.Notification{Offset: offset}:
			default:
				// Channel full, skip (subscriber will get it on next read)
			}
		}
	}
}

// NotifyError sends an error notification to all subscribers.
func (n *MemoryNotifier) NotifyError(url string, err error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.closed {
		return
	}

	subs, ok := n.subscribers[url]
	if !ok {
		return
	}

	for ch := range subs {
		select {
		case ch <- storage.Notification{Error: err}:
		default:
		}
	}
}

// Close shuts down the notifier and closes all subscription channels.
func (n *MemoryNotifier) Close() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.closed {
		return
	}

	n.closed = true

	for _, subs := range n.subscribers {
		for ch := range subs {
			close(ch)
		}
	}
	n.subscribers = nil
}
