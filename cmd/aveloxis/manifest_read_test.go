// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/collector"
	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The child an analysis starts is `aveloxis manifest-read`
// (internal/collector/manifest_child.go): the command is there under the
// name the collector starts, hidden, with no arguments and no flag of its
// own, and it is not one a person is shown.
func TestTheManifestReaderIsAHiddenSubcommand(t *testing.T) {
	root := newRootCmd()
	cmd, rest, err := root.Find([]string{collector.ManifestChildArg})
	if err != nil || cmd == root || len(rest) != 0 {
		t.Fatalf("`aveloxis %s` is not a command (%v, rest %v)", collector.ManifestChildArg, err, rest)
	}
	if !cmd.Hidden {
		t.Error("the manifest reader is shown in --help: it is not a command for a person")
	}
	if cmd.Flags().HasFlags() {
		t.Error("the manifest reader takes a flag of its own: its request arrives on standard input")
	}
	if err := cmd.Args(cmd, []string{"x"}); err == nil {
		t.Error("the manifest reader accepts an argument")
	}
	var help bytes.Buffer
	root.SetOut(&help)
	root.SetArgs([]string{"--help"})
	if err := root.Execute(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(help.String(), collector.ManifestChildArg) {
		t.Error("--help lists the manifest reader")
	}
}

// The two limits reach the collector from the configuration file, and the
// log says the values in force.
func TestTheManifestLimitsAreInstalledFromTheConfiguration(t *testing.T) {
	restoreOutboundPolicy(t) // the wiring rule for a test that names loadConfig; this one reads its source
	for _, c := range []struct{ collection, wantCPU, wantMem string }{
		{``, "manifest_read_cpu_seconds=120", "manifest_read_memory_mb=1024"},
		{`"manifest_read_cpu_seconds": 45, "manifest_read_memory_mb": 300`, "manifest_read_cpu_seconds=45", "manifest_read_memory_mb=300"},
	} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(`{"collection":{`+c.collection+`}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		var log bytes.Buffer
		installManifestLimits(cfg, slog.New(slog.NewTextHandler(&log, nil)))
		if !strings.Contains(log.String(), c.wantCPU) || !strings.Contains(log.String(), c.wantMem) {
			t.Errorf("{%s}: the log does not give the limits in force (%s, %s): %s", c.collection, c.wantCPU, c.wantMem, log.String())
		}
	}
	collector.SetManifestLimits(collector.ManifestLimits{})
	// loadConfig is what installs them, on both of its paths.
	src := srctest.StripGoComments(srctest.PackageFunc(t, "cmd/aveloxis", "func loadConfig("))
	if n := strings.Count(src, "installManifestLimits(cfg, logger)"); n != 2 {
		t.Errorf("loadConfig installs the manifest limits %d times; want once for a file that loads and once for the defaults", n)
	}
}
