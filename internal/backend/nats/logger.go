package nats


import (
	"os"
	"fmt"
	"github.com/restic/restic/internal/debug"
	"github.com/Fishwaldo/go-logadapter"
)

/* compile check against our logAdapter interface */

var _ logadapter.Logger = (*resticLogger)(nil)

/* a custom logger implementation just for Restic */
type resticLogger struct {
	
}

// Log a Trace Message
func (l *resticLogger) Trace(message string, params ...interface{}) {
	debug.Log(message, params...)
}
// Log a Debug Message
func (l *resticLogger) Debug(message string, params ...interface{}) {
	debug.Log(message, params...)
}
// Log a Info Message
func (l *resticLogger) Info(message string, params ...interface{}) {
	debug.Log(message, params...)
}
// Log a Warn Message
func (l *resticLogger) Warn(message string, params ...interface{}) {
	debug.Log(message, params...)
}
// Log a Error Message
func (l *resticLogger) Error(message string, params ...interface{}) {
	fmt.Printf("Nats Error: %s\n", fmt.Sprintf(message, params...))
	debug.Log(message, params...)
}
// Log a Fatal Message (some implementations may call os.exit() here)
func (l *resticLogger) Fatal(message string, params ...interface{}) {
	fmt.Printf("Nats Fatal: %s\n", fmt.Sprintf(message, params...))
	debug.Log(message, params...)
	os.Exit(-1)
}
// Log a Panic Message (some implmentations may call Panic)
func (l *resticLogger) Panic(message string, params ...interface{}) {
	fmt.Printf("Nats Panic: %s\n", fmt.Sprintf(message, params...))
	debug.Log(message, params...)
	panic(fmt.Sprintf(message, params...))
}
// Create a New Logger Instance with Name
func (l *resticLogger) New(name string) (logadapter.Logger) {
	return l
}
// Add Key/Value Pairs for Structured Logging and return a new Logger
func (l *resticLogger) With(key string, value interface{}) ( logadapter.Logger) {
	return l
}
// Set the Log Prefix
func (l *resticLogger) SetPrefix(name string) {
}
// Get the Log Prefix
func (l *resticLogger) GetPrefix() (string) {
	return ""
}
// Set Logging Level
func (l *resticLogger) SetLevel(logadapter.Log_Level) {

}
// Get Logging Level
func(l *resticLogger) GetLevel() (logadapter.Log_Level) {
	return logadapter.LOG_TRACE
}
// Sync/Flush the Log Buffers 
func (l *resticLogger) Sync() {

}
