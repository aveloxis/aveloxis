// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/url"
	"strings"
	"testing"
	"time"
)

// TestTransportErrorsCarryNoSearchedAddress — v0.29.71 review round 1 F1: a
// request that fails in transport returns a *url.Error whose text quotes
// the full request URL, query included, so a timeout on the user search
// logged the author's email in the retry WARN's error attribute (and in
// the exhausted-retries error) next to the redacted url attribute.
// RedactTransportError rebuilds it with the redacted URL; errors.Is/As on
// the inner error still work.
func TestTransportErrorsCarryNoSearchedAddress(t *testing.T) {
	// A closed port: every attempt fails in transport.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	_ = ln.Close()

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	c := NewHTTPClient(base, NewKeyPool([]string{"k"}, logger), logger, AuthGitHub)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, gerr := c.Get(ctx, "/search/users?q=jane.doe%40example.org+in:email&per_page=1")
	if gerr == nil {
		t.Fatal("want a transport error")
	}
	for where, text := range map[string]string{"log": logs.String(), "error": gerr.Error()} {
		if strings.Contains(text, "jane.doe") {
			t.Errorf("the %s carries the searched address:\n%s", where, text)
		}
	}

	inner := errors.New("connection refused")
	red := RedactTransportError(&url.Error{Op: "Get", URL: "https://api.github.com/search/users?q=a%40b.org", Err: inner})
	var ue *url.Error
	if !errors.As(red, &ue) || !errors.Is(red, inner) || strings.Contains(red.Error(), "a%40b") {
		t.Errorf("redacted transport error = %v; want a *url.Error without the address, wrapping the cause", red)
	}
	if RedactTransportError(nil) != nil || RedactTransportError(inner) != inner {
		t.Error("nil and non-url errors pass through unchanged")
	}
}
