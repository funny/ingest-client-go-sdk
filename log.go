package client

import (
	"encoding/json"
	"io"
	"log"
	"os"
)

var (
	DefaultLogger = NewLogger(os.Stderr, LevelWarn)

	LevelTrace LogLevel = 50
	LevelDebug LogLevel = 40
	LevelInfo  LogLevel = 30
	LevelWarn  LogLevel = 20
	LevelError LogLevel = 10
)

type LogLevel int

type Logger interface {
	WithField(name string, value interface{}) Logger
	Trace(msg string)
	Debug(msg string)
	Info(msg string)
	Warn(msg string)
	Error(msg string)
}

type logger struct {
	Writer io.Writer
	fields []func(map[string]interface{})
	logger *log.Logger
	level  LogLevel
}

func NewLogger(w io.Writer, lvl LogLevel) *logger {
	return &logger{logger: log.New(w, "", log.LstdFlags), level: lvl}
}

var _ Logger = &logger{}

func (l *logger) WithField(name string, value interface{}) Logger {
	newL := *l
	newL.fields = append(newL.fields, func(m map[string]interface{}) {
		m[name] = value
	})
	return &newL
}

func (l *logger) Trace(msg string) { l.log(LevelTrace, msg) }
func (l *logger) Debug(msg string) { l.log(LevelDebug, msg) }
func (l *logger) Info(msg string)  { l.log(LevelInfo, msg) }
func (l *logger) Warn(msg string)  { l.log(LevelWarn, msg) }
func (l *logger) Error(msg string) { l.log(LevelError, msg) }

func (l *logger) log(lvl LogLevel, msg string) {
	if lvl > l.level {
		return
	}

	m := make(map[string]interface{}, len(l.fields))
	for _, f := range l.fields {
		f(m)
	}

	var (
		b   []byte
		err error
	)
	if len(l.fields) > 0 {
		b, err = json.Marshal(m)
		if err != nil {
			println("Logger error: " + err.Error())
			return
		}
	} else {
		b = []byte(`{}`)
	}

	var lvlStr string
	switch lvl {
	case LevelTrace:
		lvlStr = "TRACE"
	case LevelDebug:
		lvlStr = "DEBUG"
	case LevelInfo:
		lvlStr = "INFO"
	case LevelWarn:
		lvlStr = "WARN"
	case LevelError:
		lvlStr = "ERROR"
	default:
		lvlStr = "LOG"
	}

	l.logger.Println(lvlStr, msg, string(b))
}
