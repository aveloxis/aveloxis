// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type rewriteAll struct {
	target *url.URL
	base   http.RoundTripper
}

func (rt rewriteAll) RoundTrip(req *http.Request) (*http.Response, error) {
	r2 := req.Clone(req.Context())
	r2.URL.Scheme, r2.URL.Host, r2.Host = rt.target.Scheme, rt.target.Host, rt.target.Host
	return rt.base.RoundTrip(r2)
}

// TestScorecardInstallShadowedIsAnError (final whole-tree review F3,
// 2026-09-28): an upgrade that writes scorecard into GoBinDir while an older
// copy comes first on PATH printed a warning and returned nil — upgrade-tools
// said "ok scorecard upgraded" and exited 0, the monthly check counted it
// updated, and serve kept running the old binary. The installer now returns
// ErrInstallShadowed naming both paths, so every caller counts a failure.
func TestScorecardInstallShadowedIsAnError(t *testing.T) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not on PATH")
	}
	var tgz bytes.Buffer
	gz := gzip.NewWriter(&tgz)
	tw := tar.NewWriter(gz)
	payload := []byte("#!/bin/sh\necho new\n")
	if err := tw.WriteHeader(&tar.Header{Name: "scorecard", Mode: 0o755, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(payload); err != nil {
		t.Fatal(err)
	}
	tw.Close()
	gz.Close()
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/releases/latest") {
			fmt.Fprint(w, `{"tag_name":"v9.9.9"}`)
			return
		}
		_, _ = w.Write(tgz.Bytes())
	}))
	defer fixture.Close()
	target, _ := url.Parse(fixture.URL)
	saved := toolFetchClient
	toolFetchClient = &http.Client{Timeout: time.Minute, Transport: rewriteAll{target: target, base: http.DefaultTransport}}
	t.Cleanup(func() { toolFetchClient = saved })

	early, gobin := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(early, "scorecard"), []byte("#!/bin/sh\necho old\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOBIN", gobin)
	t.Setenv("PATH", early+string(os.PathListSeparator)+filepath.Dir(goBin))

	err = installScorecardBinary(context.Background())
	if !errors.Is(err, ErrInstallShadowed) {
		t.Fatalf("err = %v; want ErrInstallShadowed for a copy written behind an older one on PATH", err)
	}
	for _, p := range []string{filepath.Join(gobin, "scorecard"), filepath.Join(early, "scorecard")} {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("err %q does not name %s", err, p)
		}
	}
	if _, statErr := os.Stat(filepath.Join(gobin, "scorecard")); statErr != nil {
		t.Errorf("the new copy was not written: %v", statErr)
	}

	// Unshadowed: the written copy is the one PATH finds.
	t.Setenv("PATH", gobin+string(os.PathListSeparator)+filepath.Dir(goBin))
	if err := installScorecardBinary(context.Background()); err != nil {
		t.Errorf("an unshadowed install failed: %v", err)
	}
}
