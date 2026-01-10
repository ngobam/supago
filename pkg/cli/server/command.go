package server

import (
	"github.com/rosfandy/supago/internal/config"
	"github.com/rosfandy/supago/pkg/logger"
)

var ServerLogger = logger.HcLog().Named("supago.server")

func Run() {
	cfg, err := config.LoadConfig(nil)
	if err != nil {
		ServerLogger.Error(err.Error())
		return
	}

	server := config.NewServer(cfg)
	server.RunHttpServer()
}
