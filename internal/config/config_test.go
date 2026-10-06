package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	for _, name := range []string{"SERVICE_NAME", "HTTP_ADDR", "DB_DSN", "NATS_URL", "NATS_STREAM_NAME", "NATS_ACK_WAIT_SEC", "NATS_MAX_DELIVER"} {
		t.Setenv(name, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "saruman" || cfg.HTTPAddr != ":8082" || cfg.MaxDeliver != 5 {
		t.Fatalf("unexpected defaults: %#v", cfg)
	}
}

func TestLoadRejectsInvalidMaxDeliver(t *testing.T) {
	t.Setenv("NATS_MAX_DELIVER", "0")
	if _, err := Load(); err == nil {
		t.Fatal("expected invalid max deliver error")
	}
}
