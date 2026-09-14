// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Database.Host != "localhost" {
		t.Errorf("Database.Host = %q, want %q", cfg.Database.Host, "localhost")
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("Database.Port = %d, want %d", cfg.Database.Port, 5432)
	}
	if cfg.Database.User != "augur" {
		t.Errorf("Database.User = %q, want %q", cfg.Database.User, "augur")
	}
	if cfg.Database.DBName != "augur" {
		t.Errorf("Database.DBName = %q, want %q", cfg.Database.DBName, "augur")
	}
	if cfg.Database.SSLMode != "prefer" {
		t.Errorf("Database.SSLMode = %q, want %q", cfg.Database.SSLMode, "prefer")
	}
	if cfg.Collection.Workers != 12 {
		t.Errorf("Collection.Workers = %d, want %d", cfg.Collection.Workers, 12)
	}
	if cfg.Collection.DaysUntilRecollect != 1 {
		t.Errorf("Collection.DaysUntilRecollect = %d, want %d", cfg.Collection.DaysUntilRecollect, 1)
	}
	if cfg.GitHub.BaseURL != "https://api.github.com" {
		t.Errorf("GitHub.BaseURL = %q, want %q", cfg.GitHub.BaseURL, "https://api.github.com")
	}
	if cfg.GitLab.BaseURL != "https://gitlab.com/api/v4" {
		t.Errorf("GitLab.BaseURL = %q, want %q", cfg.GitLab.BaseURL, "https://gitlab.com/api/v4")
	}
}

func TestDatabaseConfig_ConnectionString(t *testing.T) {
	db := DatabaseConfig{
		Host:     "db.example.com",
		Port:     5433,
		User:     "myuser",
		Password: "secret",
		DBName:   "mydb",
		SSLMode:  "require",
	}

	got := db.ConnectionString()
	want := "postgres://myuser:secret@db.example.com:5433/mydb?sslmode=require"
	if got != want {
		t.Errorf("ConnectionString() = %q, want %q", got, want)
	}
}

func TestDatabaseConfig_ConnectionString_DefaultSSLMode(t *testing.T) {
	db := DatabaseConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "augur",
		Password: "pass",
		DBName:   "augur",
		// SSLMode intentionally empty
	}

	got := db.ConnectionString()
	if !strings.Contains(got, "sslmode=prefer") {
		t.Errorf("ConnectionString() = %q, expected sslmode=prefer when SSLMode is empty", got)
	}
}

func TestLoad_NonexistentFile(t *testing.T) {
	_, err := Load("/tmp/nonexistent_aveloxis_config_test.json")
	if err == nil {
		t.Fatal("Load() with nonexistent file should return error")
	}
}

func TestLoad_ValidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	data := []byte(`{
		"database": {
			"host": "remotehost",
			"port": 5433,
			"user": "testuser",
			"password": "testpass",
			"dbname": "testdb",
			"sslmode": "require"
		},
		"github": {
			"api_keys": ["ghp_abc123"]
		},
		"collection": {
			"workers": 8,
			"days_until_recollect": 7
		}
	}`)

	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Database.Host != "remotehost" {
		t.Errorf("Database.Host = %q, want %q", cfg.Database.Host, "remotehost")
	}
	if cfg.Database.Port != 5433 {
		t.Errorf("Database.Port = %d, want %d", cfg.Database.Port, 5433)
	}
	if cfg.Collection.Workers != 8 {
		t.Errorf("Collection.Workers = %d, want %d", cfg.Collection.Workers, 8)
	}
	if len(cfg.GitHub.APIKeys) != 1 || cfg.GitHub.APIKeys[0] != "ghp_abc123" {
		t.Errorf("GitHub.APIKeys = %v, want [ghp_abc123]", cfg.GitHub.APIKeys)
	}
}

func TestLoad_MergesWithDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.json")

	// Only set database host; everything else should come from defaults.
	data := []byte(`{"database": {"host": "custom-host"}}`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Database.Host != "custom-host" {
		t.Errorf("Database.Host = %q, want %q", cfg.Database.Host, "custom-host")
	}
	// These should be filled in from defaults.
	if cfg.Database.Port != 5432 {
		t.Errorf("Database.Port = %d, want default %d", cfg.Database.Port, 5432)
	}
	if cfg.Database.User != "augur" {
		t.Errorf("Database.User = %q, want default %q", cfg.Database.User, "augur")
	}
	if cfg.GitHub.BaseURL != "https://api.github.com" {
		t.Errorf("GitHub.BaseURL = %q, want default %q", cfg.GitHub.BaseURL, "https://api.github.com")
	}
}

// v0.28.3 (SR-10) — the history-sweep knobs, JSON → effective value.
// Three states each: absent (accessor default), explicit value,
// nonsense (falls back).
func TestActivityHistoryKnobsEndToEnd(t *testing.T) {
	// Absent → defaults.
	var c CollectionConfig
	if c.ActivityHistoryIntervalValue() != time.Minute ||
		c.ActivityHistoryBatchValue() != 150 ||
		c.ActivityHistoryConcurrencyValue() != 8 ||
		c.ActivityHistoryWindowConcurrencyValue() != 4 ||
		c.ActivityHistoryCooldownValue() != 90*24*time.Hour {
		t.Error("absent knobs must yield the documented defaults (1m/150/8/4/90d)")
	}
	// Explicit JSON values flow through.
	var cfg Config
	if err := json.Unmarshal([]byte(`{"collection":{
		"activity_history_interval_minutes": 5,
		"activity_history_batch": 300,
		"activity_history_concurrency": 16,
		"activity_history_window_concurrency": 2,
		"activity_history_cooldown_days": 30}}`), &cfg); err != nil {
		t.Fatal(err)
	}
	cc := cfg.Collection
	if cc.ActivityHistoryIntervalValue() != 5*time.Minute ||
		cc.ActivityHistoryBatchValue() != 300 ||
		cc.ActivityHistoryConcurrencyValue() != 16 ||
		cc.ActivityHistoryWindowConcurrencyValue() != 2 ||
		cc.ActivityHistoryCooldownValue() != 30*24*time.Hour {
		t.Errorf("explicit JSON knobs must flow to the accessors: %+v", cc)
	}
	// Nonsense falls back (never zero-duration tickers).
	c = CollectionConfig{ActivityHistoryIntervalMinutes: -1, ActivityHistoryConcurrency: -3}
	if c.ActivityHistoryIntervalValue() != time.Minute || c.ActivityHistoryConcurrencyValue() != 8 {
		t.Error("negative knobs must fall back to defaults")
	}
}
