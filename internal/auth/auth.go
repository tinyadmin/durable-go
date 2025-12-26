package auth

import (
	"context"
	"net/http"
)

// Operation represents a stream operation for authorization.
type Operation string

const (
	OpCreate Operation = "create"
	OpAppend Operation = "append"
	OpRead   Operation = "read"
	OpDelete Operation = "delete"
)

// AuthProvider defines the interface for authentication and authorization.
type AuthProvider interface {
	// Authenticate validates credentials and returns identity.
	// Returns error if authentication fails.
	Authenticate(r *http.Request) (*AuthContext, error)

	// Authorize checks if the identity can perform operation on stream.
	// Returns error if authorization fails.
	Authorize(ctx *AuthContext, op Operation, streamURL string) error
}

// AuthContext holds the authenticated identity information.
type AuthContext struct {
	TenantID string
	UserID   string
	Extra    map[string]any // custom claims (roles, scopes, etc.)
}

// Context key type for type safety.
type contextKey string

const authContextKey contextKey = "auth"

// FromContext extracts AuthContext from request context.
// Returns nil if no auth context is set.
func FromContext(ctx context.Context) *AuthContext {
	if v := ctx.Value(authContextKey); v != nil {
		return v.(*AuthContext)
	}
	return nil
}

// WithContext adds AuthContext to the context.
func WithContext(ctx context.Context, auth *AuthContext) context.Context {
	return context.WithValue(ctx, authContextKey, auth)
}
