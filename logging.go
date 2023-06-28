package eplidr

import "log"

type Logger interface {
	Debug(v ...any)
	Info(v ...any)
	Error(v ...any)
	Warn(v ...any)
}

var logger Logger = nil

func SetLogger(newLogger Logger) {
	logger = newLogger
}

type DefaultLogger struct{}

func (l *DefaultLogger) Info(v ...any) {
	log.Println(v...)
}
func (l *DefaultLogger) Debug(v ...any) {
	log.Println(v...)
}
func (l *DefaultLogger) Error(v ...any) {
	log.Println(v...)
}
func (l *DefaultLogger) Warn(v ...any) {
	log.Println(v...)
}
