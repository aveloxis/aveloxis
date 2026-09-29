// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.29.70 whole-branch review: isTransientNetError decided on the error's
// text, which carries the probed URL — a repository named
// "…connection-refused…" whose probe failed any other way was retried as a
// network blip. It decides on the net package's typed errors now (SR-5).

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"time"
)

func TestIsTransientNetErrorIsTyped(t *testing.T) {
	wrap := func(e error) error {
		return &url.Error{Op: "Head", URL: "https://github.com/o/r", Err: e}
	}
	for name, e := range map[string]error{
		"dns":         wrap(&net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "github.com", IsNotFound: true}}),
		"refused":     wrap(&net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}),
		"unreachable": wrap(&net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", syscall.ENETUNREACH)}),
		"i/o timeout": wrap(&net.OpError{Op: "read", Net: "tcp", Err: timeoutErr{}}),
	} {
		if !isTransientNetError(e) {
			t.Errorf("%s: %v must be transient", name, e)
		}
	}
	// The URL names every old needle; the failure itself is not a network blip.
	named := &url.Error{Op: "Head", URL: "https://github.com/no-such-host/connection-refused-network-is-unreachable-i-o-timeout",
		Err: errors.New("x509: certificate signed by unknown authority")}
	if isTransientNetError(named) {
		t.Errorf("%v: a repository's NAME is not a network failure", named)
	}
	if isTransientNetError(fmt.Errorf("plain")) {
		t.Error("an untyped error is not transient")
	}
}

// A real refused connection classifies through the typed path.
func TestIsTransientNetErrorOnARealRefusedDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodHead, "http://"+addr+"/o/r", nil)
	_, err = http.DefaultClient.Do(req)
	if err == nil {
		t.Skip("the closed port answered")
	}
	if !isTransientNetError(err) {
		t.Errorf("a refused dial (%v) must be transient", err)
	}
}

type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }
