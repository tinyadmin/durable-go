package auth

import (
	"errors"
	"net/http"
	"strings"
)

// Common errors.
var (
	ErrMissingCredentials = errors.New("missing credentials")
	ErrInvalidCredentials = errors.New("invalid credentials")
	ErrForbidden          = errors.New("forbidden")
)

// APIKeyProvider authenticates using X-API-Key header.
type APIKeyProvider struct {
	// Validate is called with the API key to validate and return auth context.
	Validate func(key string) (*AuthContext, error)

	// Authorize is called to check if operation is allowed.
	// If nil, all operations are allowed for authenticated users.
	AuthorizeFunc func(ctx *AuthContext, op Operation, streamURL string) error
}

func (p *APIKeyProvider) Authenticate(r *http.Request) (*AuthContext, error) {
	key := r.Header.Get("X-API-Key")
	if key == "" {
		return nil, ErrMissingCredentials
	}
	return p.Validate(key)
}

func (p *APIKeyProvider) Authorize(ctx *AuthContext, op Operation, streamURL string) error {
	if p.AuthorizeFunc != nil {
		return p.AuthorizeFunc(ctx, op, streamURL)
	}
	return nil // allow all by default
}

// BearerProvider authenticates using Authorization: Bearer <token> header.
type BearerProvider struct {
	// Validate is called with the bearer token to validate and return auth context.
	Validate func(token string) (*AuthContext, error)

	// AuthorizeFunc is called to check if operation is allowed.
	// If nil, all operations are allowed for authenticated users.
	AuthorizeFunc func(ctx *AuthContext, op Operation, streamURL string) error
}

func (p *BearerProvider) Authenticate(r *http.Request) (*AuthContext, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return nil, ErrMissingCredentials
	}

	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return nil, ErrInvalidCredentials
	}

	return p.Validate(parts[1])
}

func (p *BearerProvider) Authorize(ctx *AuthContext, op Operation, streamURL string) error {
	if p.AuthorizeFunc != nil {
		return p.AuthorizeFunc(ctx, op, streamURL)
	}
	return nil // allow all by default
}
