package config

import (
	"fmt"

	"github.com/spf13/viper"
)

var AppConfig *Config

type Config struct {
	ServerHost               string `mapstructure:"SERVER_HOST"`
	ServerPort               string `mapstructure:"SERVER_PORT"`
	SupabaseProjectId        string `mapstructure:"SUPABASE_PROJECT_ID"`
	SupabaseApiKey           string `mapstructure:"SUPABASE_API_KEY"`
	SupabaseAnonKey          string `mapstructure:"SUPABASE_ANON_KEY"`
	SupabaseAccessToken      string `mapstructure:"SUPABASE_ACCESS_TOKEN"`
	MaxServerRequestBodySize int    `mapstructure:"MAX_SERVER_REQUEST_BODY_SIZE"`
}

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

	AppConfig = &cfg
	return AppConfig, nil
}

func (c *Config) Address() string {
	return fmt.Sprintf("%s%s", c.ServerHost, c.ServerPort)
}

func (c *Config) SupabaseUrl() string {
	return fmt.Sprintf("https://%s.supabase.co", c.SupabaseProjectId)
}

func (c *Config) SupabaseManagementUrl() string {
	return fmt.Sprintf("https://api.supabase.com/v1/projects/%s", c.SupabaseProjectId)
}
