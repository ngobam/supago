package logger

import (
	"os"
	"time"

	"github.com/hashicorp/go-hclog"
)

var instance hclog.Logger

func HcLog() hclog.Logger {
	if instance == nil {
		instance = hclog.New(&hclog.LoggerOptions{
			Level:           hclog.Trace,
			Color:           hclog.ForceColor,
			ColorHeaderOnly: true,
			TimeFormat:      time.RFC3339,
		})
	}

	return instance
}

func Fatal(msg string, args ...interface{}) {
	HcLog().Error(msg, args...)
	os.Exit(1)
}
