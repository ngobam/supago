package logger

import (
	"os"

	"github.com/hashicorp/go-hclog"
)

var instance hclog.Logger

func HcLog() hclog.Logger {
	if instance == nil {
		instance = hclog.NewInterceptLogger(&hclog.LoggerOptions{
			Name:   "supago",
			Level:  hclog.Trace,
			Color:  hclog.ForceColor,
			Output: os.Stderr,
		})
	}

	return instance
}

func Fatal(msg string, args ...interface{}) {
	HcLog().Error(msg, args...)
	os.Exit(1)
}
