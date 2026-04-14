package logger

import (
	"context"
	"net/http"

	"go.uber.org/zap"
)

type loggerKey struct{}
type httpRequestKey struct{}
type logFieldsKey struct{}

func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey{}, logger)
}

func Logger(ctx context.Context) *zap.Logger {
	return ctx.Value(loggerKey{}).(*zap.Logger)
}

// WithHTTPRequest attaches stable HTTP metadata for error logs (method, path, client).
func WithHTTPRequest(ctx context.Context, r *http.Request) context.Context {
	return context.WithValue(ctx, httpRequestKey{}, httpRequestFields(r))
}

func httpRequestFields(r *http.Request) []zap.Field {
	if r == nil {
		return nil
	}
	return []zap.Field{
		zap.String("http.method", r.Method),
		zap.String("http.path", r.URL.Path),
		zap.String("http.query", truncateRunes(r.URL.RawQuery, 512)),
		zap.String("remote_addr", r.RemoteAddr),
		zap.String("http.host", r.Host),
	}
}

// HTTPRequestFields returns fields previously stored with WithHTTPRequest.
func HTTPRequestFields(ctx context.Context) []zap.Field {
	v := ctx.Value(httpRequestKey{})
	if v == nil {
		return nil
	}
	return v.([]zap.Field)
}

// WithLogFields merges additional structured fields into the request context for error logging.
func WithLogFields(ctx context.Context, fields ...zap.Field) context.Context {
	var prev []zap.Field
	if v := ctx.Value(logFieldsKey{}); v != nil {
		prev = v.([]zap.Field)
	}
	merged := make([]zap.Field, 0, len(prev)+len(fields))
	merged = append(merged, prev...)
	merged = append(merged, fields...)
	return context.WithValue(ctx, logFieldsKey{}, merged)
}

// LogFields returns extra fields from WithLogFields.
func LogFields(ctx context.Context) []zap.Field {
	if v := ctx.Value(logFieldsKey{}); v != nil {
		return v.([]zap.Field)
	}
	return nil
}

func truncateRunes(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
