package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
)

type Config struct {
	ServiceName string
	HTTPAddr    string
	DBDSN       string
	NATSURL     string
	NATSStream  string
	AckWait     time.Duration
	MaxDeliver  int
	// Payout liveness knobs (Phase C, T13): payout leases bound the
	// provider-attempt window (Submit/Retry raise it); the sweeper flags
	// REQUESTED/PROCESSING rows whose reservation/lease predates 2x the lease.
	PayoutLease         time.Duration
	PayoutSweepInterval time.Duration
	// Nightly withdrawal batching (D-062): the worker ticks every
	// NightlyBatchCheckInterval while the UTC hour matches
	// NightlyBatchHourUTC and sweeps each streamer's total available balance
	// into one DRAFT batch created as NightlyBatchActorAdminID.
	NightlyBatchEnabled       bool
	NightlyBatchHourUTC       int
	NightlyBatchCheckInterval time.Duration
	NightlyBatchActorAdminID  uuid.UUID
}

func Load() (Config, error) {
	cfg := Config{
		ServiceName:         value("SERVICE_NAME", "saruman"),
		HTTPAddr:            value("HTTP_ADDR", ":8082"),
		DBDSN:               value("DB_DSN", "postgres://inflora:dev@localhost:5432/inflora?sslmode=disable"),
		NATSURL:             value("NATS_URL", "nats://localhost:4222"),
		NATSStream:          value("NATS_STREAM_NAME", "inflora-events"),
		AckWait:             durationSeconds("NATS_ACK_WAIT_SEC", 30),
		MaxDeliver:          integer("NATS_MAX_DELIVER", 5),
		PayoutLease:         durationSeconds("PAYOUT_LEASE_SEC", 900),
		PayoutSweepInterval: durationSeconds("PAYOUT_SWEEP_INTERVAL_SEC", 60),
		NightlyBatchEnabled:       boolean("NIGHTLY_BATCH_ENABLED", false),
		NightlyBatchHourUTC:       integer("NIGHTLY_BATCH_HOUR_UTC", 0),
		NightlyBatchCheckInterval: durationSeconds("NIGHTLY_BATCH_CHECK_INTERVAL_SEC", 60),
	}
	if cfg.ServiceName == "" || cfg.HTTPAddr == "" || cfg.DBDSN == "" || cfg.NATSURL == "" || cfg.NATSStream == "" {
		return Config{}, fmt.Errorf("config: service, HTTP, database, NATS URL, and NATS stream are required")
	}
	if cfg.MaxDeliver < 1 {
		return Config{}, fmt.Errorf("config: NATS_MAX_DELIVER must be positive")
	}
	if cfg.NightlyBatchHourUTC < 0 || cfg.NightlyBatchHourUTC > 23 {
		return Config{}, fmt.Errorf("config: NIGHTLY_BATCH_HOUR_UTC must be 0-23")
	}
	actorID, err := uuid.Parse(value("NIGHTLY_BATCH_ACTOR_ADMIN_ID", "50000000-0000-4000-8000-000000000001"))
	if err != nil {
		return Config{}, fmt.Errorf("config: NIGHTLY_BATCH_ACTOR_ADMIN_ID: %w", err)
	}
	cfg.NightlyBatchActorAdminID = actorID
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

func boolean(name string, fallback bool) bool {
	v, err := strconv.ParseBool(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return v
}

func durationSeconds(name string, fallback int) time.Duration {
	return time.Duration(integer(name, fallback)) * time.Second
}
