package notify

import "github.com/me/durable/storage"

// Notifier handles pub/sub for stream updates.
type Notifier interface {
	// Subscribe registers for notifications on a stream.
	// Returns a channel that will receive notifications when new data is available.
	Subscribe(url string, offset string) <-chan storage.Notification

	// Unsubscribe removes a notification subscription.
	Unsubscribe(url string, ch <-chan storage.Notification)

	// Notify sends a notification to all subscribers of a stream.
	Notify(url string, offset string)

	// NotifyError sends an error notification to all subscribers.
	NotifyError(url string, err error)

	// Close shuts down the notifier and closes all subscription channels.
	Close()
}
