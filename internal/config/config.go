package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName string
	HTTPAddr    string
	DBDSN       string
	NATSURL     string
	NATSStream  string
	AckWait     time.Duration
	MaxDeliver  int
}

func Load() (Config, error) {
	cfg := Config{
		ServiceName: value("SERVICE_NAME", "saruman"),
		HTTPAddr:    value("HTTP_ADDR", ":8082"),
		DBDSN:       value("DB_DSN", "postgres://inflora:dev@localhost:5432/inflora?sslmode=disable"),
		NATSURL:     value("NATS_URL", "nats://localhost:4222"),
		NATSStream:  value("NATS_STREAM_NAME", "inflora-events"),
		AckWait:     durationSeconds("NATS_ACK_WAIT_SEC", 30),
		MaxDeliver:  integer("NATS_MAX_DELIVER", 5),
	}
	if cfg.ServiceName == "" || cfg.HTTPAddr == "" || cfg.DBDSN == "" || cfg.NATSURL == "" || cfg.NATSStream == "" {
		return Config{}, fmt.Errorf("config: service, HTTP, database, NATS URL, and NATS stream are required")
	}
	if cfg.MaxDeliver < 1 {
		return Config{}, fmt.Errorf("config: NATS_MAX_DELIVER must be positive")
	}
	return cfg, nil
}

func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func integer(name string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return v
}

func durationSeconds(name string, fallback int) time.Duration {
	return time.Duration(integer(name, fallback)) * time.Second
}
