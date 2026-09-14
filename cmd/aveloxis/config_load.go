// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"errors"
	"io/fs"
	"log/slog"
	"os"

	"github.com/aveloxis/aveloxis/internal/config"
)

func loadConfig(cfgPath string, logger *slog.Logger) *config.Config {
	cfg, usedDefaults, err := resolveConfig(cfgPath)
	if err != nil {
		logger.Error("invalid config — refusing to run on defaults", "path", cfgPath, "error", err)
		os.Exit(2)
	}
	if usedDefaults {
		logger.Warn("config file not found, using defaults", "path", cfgPath)
	}
	return cfg
}

// resolveConfig loads the config file. Only a MISSING file falls back to
// the compiled defaults (usedDefaults); every other error — unreadable,
// malformed JSON, or a config the loader rejects such as an invalid
// gitlab.instances block — is returned. v0.30.0: running on defaults after
// a rejected config silently dropped the operator's GitLab instances and
// keys.
func resolveConfig(cfgPath string) (cfg *config.Config, usedDefaults bool, err error) {
	cfg, err = config.Load(cfgPath)
	if err == nil {
		return cfg, false, nil
	}
	if errors.Is(err, fs.ErrNotExist) {
		return config.DefaultConfig(), true, nil
	}
	return nil, false, err
}
