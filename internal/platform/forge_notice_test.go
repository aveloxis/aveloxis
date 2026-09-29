// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Worklist item 82: the repository page repeats the forge's own message
// for a blocked or disabled repository. These pin the capture: the
// client's 451 and 403 arms carry GitHub's block object on the error they
// return, reachable through NoticeOf, and an ordinary 403 carries none.

func noticeServer(t *testing.T, status int, body string) *HTTPClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewHTTPClient(srv.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
}

func TestLegalBlockCarriesTheForgeNotice(t *testing.T) {
	c := noticeServer(t, http.StatusUnavailableForLegalReasons,
		`{"message":"Repository access blocked","block":{"reason":"dmca","html_url":"https://github.com/github/dmca/blob/master/2025/11/x.md"}}`)
	_, err := c.Get(context.Background(), "/repos/o/r")
	n, ok := NoticeOf(err)
	if !ok {
		t.Fatalf("NoticeOf(%v) = false, want the block notice", err)
	}
	if n.Message != "Repository access blocked" || n.Reason != "dmca" ||
		n.URL != "https://github.com/github/dmca/blob/master/2025/11/x.md" {
		t.Errorf("notice = %+v", n)
	}
	// The sentinels the gone path keys on must survive the wrap.
	if !errors.Is(err, ErrGone) || !errors.Is(err, ErrLegallyBlocked) {
		t.Errorf("err = %v, want ErrGone and ErrLegallyBlocked still reachable", err)
	}
}

func TestForbiddenWithBlockCarriesTheForgeNotice(t *testing.T) {
	c := noticeServer(t, http.StatusForbidden,
		`{"message":"Repository access blocked","block":{"reason":"tos","html_url":"https://github.com/tos"}}`)
	_, err := c.Get(context.Background(), "/repos/o/r")
	n, ok := NoticeOf(err)
	if !ok {
		t.Fatalf("NoticeOf(%v) = false, want the block notice", err)
	}
	if n.Message != "Repository access blocked" || n.Reason != "tos" || n.URL != "https://github.com/tos" {
		t.Errorf("notice = %+v", n)
	}
	// Not re-classified: the sideline decision waits for the probe
	// (summary/40 §9), so a blocked 403 stays the forbidden it was.
	if !errors.Is(err, ErrForbidden) {
		t.Errorf("err = %v, want ErrForbidden unchanged", err)
	}
	if errors.Is(err, ErrGone) {
		t.Errorf("err = %v: a 403 must not become gone here", err)
	}
}

func TestForbiddenWithoutBlockCarriesNoNotice(t *testing.T) {
	for _, body := range []string{
		`{"message":"Resource not accessible by personal access token"}`,
		`not json`,
		`{"message":"x","block":null}`,
	} {
		c := noticeServer(t, http.StatusForbidden, body)
		_, err := c.Get(context.Background(), "/repos/o/r")
		if n, ok := NoticeOf(err); ok {
			t.Errorf("body %q: NoticeOf = %+v, want none — only a block object is the forge's notice", body, n)
		}
		if !errors.Is(err, ErrForbidden) {
			t.Errorf("body %q: err = %v, want ErrForbidden", body, err)
		}
	}
}

func TestNoticeOfSurvivesWrapping(t *testing.T) {
	inner := &NoticeError{Notice: ForgeNotice{Message: "m"}, Err: ErrForbidden}
	wrapped := fmt.Errorf("phase 0: %w", inner)
	if n, ok := NoticeOf(wrapped); !ok || n.Message != "m" {
		t.Errorf("NoticeOf(wrapped) = %+v, %v", n, ok)
	}
	if _, ok := NoticeOf(nil); ok {
		t.Error("NoticeOf(nil) = true")
	}
	if _, ok := NoticeOf(errors.New("plain")); ok {
		t.Error("NoticeOf(plain) = true")
	}
}

func TestForgeNoticeText(t *testing.T) {
	cases := []struct {
		n    ForgeNotice
		want string
	}{
		{ForgeNotice{Message: "Repository access blocked", Reason: "dmca"}, "Repository access blocked (dmca)"},
		{ForgeNotice{Message: "Access disabled"}, "Access disabled"},
		{ForgeNotice{Reason: "tos"}, "tos"},
		{ForgeNotice{}, ""},
	}
	for _, c := range cases {
		if got := c.n.Text(); got != c.want {
			t.Errorf("%+v.Text() = %q, want %q", c.n, got, c.want)
		}
	}
}
