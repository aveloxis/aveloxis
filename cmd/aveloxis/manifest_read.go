// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"log/slog"
	"os"
	"time"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/spf13/cobra"
)

// manifestReadCmd is the child an analysis starts to read one repository's
// dependency manifests (0.29.213; internal/collector/manifest_child.go). It
// is not a command for a person: it takes its one request on standard input,
// answers on standard output, and reads no configuration, which is why it is
// hidden and takes no flag of its own.
func manifestReadCmd() *cobra.Command {
	return &cobra.Command{
		Use:    collector.ManifestChildArg,
		Short:  "Internal: read one repository's dependency manifests for an analysis",
		Hidden: true,
		Args:   cobra.NoArgs,
		Run: func(*cobra.Command, []string) {
			os.Exit(collector.ManifestChildMain(os.Stdin, os.Stdout))
		},
	}
}

// installManifestLimits gives the collector the limits on that child, and
// says the EFFECTIVE values where they are installed.
func installManifestLimits(cfg *config.Config, logger *slog.Logger) {
	cpu, heap := cfg.Collection.ManifestReadCPULimit(), cfg.Collection.ManifestReadMemoryLimit()
	collector.SetManifestLimits(collector.ManifestLimits{CPU: cpu, HeapBytes: heap})
	logger.Info("manifest reader limits", "manifest_read_cpu_seconds", int(cpu/time.Second), "manifest_read_memory_mb", heap>>20)
}
