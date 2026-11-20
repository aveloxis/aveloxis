// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/hostwatch"
)

// The threat list is fetched over https only: a plain-http list is refused
// with no request sent, and every host is then "not known", never clear.
func TestThreatListIsHTTPSOnlyAndUnknownWhenUnusable(t *testing.T) {
	var asked atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		asked.Add(1)
		_, _ = io.WriteString(w, "evil.example.com\n")
	}))
	t.Cleanup(srv.Close)
	check := RepositoryHostThreatList(srv.URL+"/list.txt")(context.Background(), []string{"repo.evil.example.com", "clean.example.org"})
	for _, h := range []string{"repo.evil.example.com", "clean.example.org"} {
		if o := check(h); o.Kind != hostwatch.KindThreatList || o.State != hostwatch.Unknown {
			t.Errorf("%s with an unusable list: %+v; want not known", h, o)
		}
	}
	if n := asked.Load(); n != 0 {
		t.Errorf("the plain-http list was requested %d times", n)
	}
	if _, err := fetchThreatList(context.Background(), "https://user:pw@lists.example/x.txt", nil, 10); !errors.Is(err, errThreatListNotHTTPS) {
		t.Errorf("a URL with credentials: %v", err)
	}
}

// The bound: a body at it is read; one byte past it is "too long", not a
// shorter list silently matched.
func TestLimitedBodyReportsACut(t *testing.T) {
	read := func(s string, n int64) (string, bool) {
		l := &limitedBody{r: strings.NewReader(s), n: n}
		b, _ := io.ReadAll(l)
		return string(b), l.cut
	}
	if got, cut := read("abcdef", 6); got != "abcdef" || cut {
		t.Errorf("a body at the bound: %q cut %v", got, cut)
	}
	if got, cut := read("abcdefg", 6); got != "abcdef" || !cut {
		t.Errorf("a body one byte past the bound: %q cut %v; want it reported as cut", got, cut)
	}
	if got, cut := read("abc", 6); got != "abc" || cut {
		t.Errorf("a short body: %q cut %v", got, cut)
	}
}
