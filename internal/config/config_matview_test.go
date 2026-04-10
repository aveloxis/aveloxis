package config

import (
	"testing"
)

func TestMatviewRebuildDayDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.MatviewRebuildDay != "saturday" {
		t.Errorf("default MatviewRebuildDay = %q, want %q", cfg.Collection.MatviewRebuildDay, "saturday")
	}
}

func TestMatviewRebuildDayDisabled(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Collection.MatviewRebuildDay = "disabled"
	if cfg.Collection.MatviewRebuildDay != "disabled" {
		t.Error("should accept 'disabled'")
	}
}

func TestMatviewRebuildOnStartupDefault(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Collection.MatviewRebuildOnStartup {
		t.Error("default MatviewRebuildOnStartup should be false")
	}
}
