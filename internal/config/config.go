package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Config struct {
	ServerHost               string `mapstructure:"SERVER_HOST"`
	ServerPort               string `mapstructure:"SERVER_PORT"`
	MaxServerRequestBodySize int    `mapstructure:"MAX_SERVER_REQUEST_BODY_SIZE"`
}

// LoadConfig reads configuration from a YAML file and returns a Config object.
func LoadConfig(path *string) (*Config, error) {
	v := viper.New()

	if path != nil {
		v.SetConfigFile(*path)
	} else {
		v.SetConfigFile("app.yaml")
	}

	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if cfg.ServerPort != "" && cfg.ServerPort[0] != ':' {
		cfg.ServerPort = ":" + cfg.ServerPort
	}

	return &cfg, nil
}

func (c *Config) Address() string {
	return fmt.Sprintf("%s%s", c.ServerHost, c.ServerPort)
}
