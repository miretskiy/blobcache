package blobcache

import "log/slog"

// Global logger for all blobcache instances
var log = slog.Default()

// SetLogger configures the global logger
func SetLogger(l *slog.Logger) {
	log = l
}
