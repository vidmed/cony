package cony

// A Logger writes key/value pairs to a Handler
type Logger interface {
	// Log a message at the given level with context key/value pairs
	Trace(msg string, ctx ...interface{})
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

var _ Logger = noopLogger{}

type noopLogger struct{}

func (l noopLogger) Trace(msg string, ctx ...interface{}) {}
func (l noopLogger) Debug(msg string, ctx ...interface{}) {}
func (l noopLogger) Info(msg string, ctx ...interface{})  {}
func (l noopLogger) Warn(msg string, ctx ...interface{})  {}
func (l noopLogger) Error(msg string, ctx ...interface{}) {}
func (l noopLogger) Crit(msg string, ctx ...interface{})  {}
