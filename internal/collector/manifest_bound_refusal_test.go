// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Review round 8 (B8-6). A file a build's reader opens FOR ITSELF (a version
// catalogue, a properties file, an applied script) that the read bound
// refuses is there and was not read. That is not the same as its not being
// there: the build that depends on it is reported as not read. Until
// 0.29.215 the refusal only logged, and the build read as whole with what
// the file would have given missing: 1 dependency of 51 with a version, no
// failure recorded.
//
// Each shape is read through the real child twice: with room, where every
// dependency has its version; and at a limit under which the 6 MiB file
// cannot be read, where the build file must be among the failed and no
// dependency may carry a version the unread file would have changed.
func TestABuildFileTheBoundRefusesMakesItsBuildUnread(t *testing.T) {
	filler := func(format string) string {
		var b strings.Builder
		for i := 0; b.Len() < 6<<20; i++ {
			fmt.Fprintf(&b, format, i, i)
		}
		return b.String()
	}
	shapes := map[string]func(dir string) map[string]string{
		"a version catalogue": func(dir string) map[string]string {
			var cat, build strings.Builder
			cat.WriteString("[versions]\n" + filler("v%d = \"1.%d.0\"\n") + "[libraries]\n")
			build.WriteString("dependencies {\n")
			for i := 0; i < 50; i++ {
				fmt.Fprintf(&cat, "lib%d = { module = \"org.example:lib%d\", version.ref = \"v%d\" }\n", i, i, i)
				fmt.Fprintf(&build, "    implementation libs.lib%d\n", i)
			}
			build.WriteString("    implementation 'org.example:plain:1.0'\n}\n")
			return map[string]string{"gradle/libs.versions.toml": cat.String(), "settings.gradle": "rootProject.name = 'x'\n", "build.gradle": build.String()}
		},
		"an applied script": func(dir string) map[string]string {
			return map[string]string{
				"versions.gradle": "ext {\n" + filler("  filler%d = '1.%d.0'\n") + "  fooVersion = '2.0.0'\n}\n",
				"settings.gradle": "rootProject.name = 'x'\n",
				"build.gradle":    "apply from: 'versions.gradle'\ndependencies {\n    implementation \"org.example:foo:$fooVersion\"\n}\n",
			}
		},
	}
	read := func(dir string, heap int64) (failed map[string]bool, versions int, err error) {
		run, err := runManifestChild(context.Background(), manifestRequest{Passes: []string{manifestPassDeclarations}, Dir: dir, RemotePomLimit: 5},
			ManifestLimits{CPU: 120 * time.Second, HeapBytes: heap}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
		if err != nil || run == nil || run.decl == nil {
			return nil, 0, fmt.Errorf("the child gave no declarations: %v", err)
		}
		failed = map[string]bool{}
		for _, f := range run.decl.Failed {
			failed[f.Path] = true
		}
		for _, d := range run.decl.Deps {
			if d.Version != "" && d.Name != "org.example:plain" {
				versions++
			}
		}
		return failed, versions, nil
	}
	for name, files := range shapes {
		dir := t.TempDir()
		for rel, text := range files(dir) {
			path := filepath.Join(dir, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		failed, versions, err := read(dir, 1024<<20)
		if err != nil || failed["build.gradle"] || versions == 0 {
			t.Fatalf("%s, with room: err %v, build.gradle failed %v, %d versions from the file; the fixture must read whole first", name, err, failed["build.gradle"], versions)
		}
		// A 6 MiB file costs its reader three times its length; twelve
		// mebibytes cannot hold that.
		failed, versions, err = read(dir, 12<<20)
		if err != nil {
			// The child was stopped at its limit: a named failure of the
			// whole reading, which is not this defect.
			t.Logf("%s: at 12 MiB the reading was stopped: %v", name, err)
			continue
		}
		if !failed["build.gradle"] || versions != 0 {
			t.Errorf("%s, with the file refused by the read bound: build.gradle among the failed = %v, %d dependencies still carry a version it gives; want the build reported unread", name, failed["build.gradle"], versions)
		}
	}
}
