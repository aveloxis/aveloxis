// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/aveloxis/aveloxis/internal/hostwatch"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// threatListBodyLimit bounds the threat list's download. The default list
// was 11 MB (392,178 lines) on 2026-11-19; the bound is about ten times
// that. The list is streamed, not held, so the bound is against a body that
// never ends, not against memory. A longer list is "not known".
const threatListBodyLimit int64 = 128 << 20

var (
	errThreatListNotHTTPS = errors.New("the threat list is fetched over https only, from a URL with no credentials")
	errThreatListTooLong  = errors.New("the threat list is longer than its bound")
)

// limitedBody reads at most n bytes of r and reports having been cut.
type limitedBody struct {
	r   io.Reader
	n   int64
	cut bool
}

func (l *limitedBody) Read(p []byte) (int, error) {
	if l.n <= 0 {
		// Is there more? One byte tells a body at the bound from one past it.
		var one [1]byte
		if n, _ := l.r.Read(one[:]); n > 0 {
			l.cut = true
		}
		return 0, io.EOF
	}
	if int64(len(p)) > l.n {
		p = p[:l.n]
	}
	n, err := l.r.Read(p)
	l.n -= int64(n)
	return n, err
}

// RepositoryHostThreatList downloads the list at listURL and answers, for
// each of hosts, whether it is on it (0.29.191; summary/70, 4.5, D14). The
// list is matched here: no host is sent anywhere. The list's host is the
// operator's setting but is not trusted by name: https, every hop judged by
// the outbound-address rule and kept on https, the body bounded. When the
// list cannot be obtained or read, every host's answer is "not known".
func RepositoryHostThreatList(listURL string) func(ctx context.Context, hosts []string) func(host string) hostwatch.Observation {
	return func(ctx context.Context, hosts []string) func(host string) hostwatch.Observation {
		m, err := fetchThreatList(ctx, listURL, hosts, threatListBodyLimit)
		if err != nil {
			unknown := hostwatch.ThreatListUnknown(err)
			return func(string) hostwatch.Observation { return unknown }
		}
		return m.Check
	}
}

func fetchThreatList(ctx context.Context, listURL string, hosts []string, limit int64) (*hostwatch.ThreatMatches, error) {
	if u, err := url.Parse(listURL); err != nil || u.Scheme != "https" || u.User != nil {
		return nil, fmt.Errorf("%w: %s", errThreatListNotHTTPS, platform.RedactURLUserinfo(listURL))
	}
	resp, err := doRegistryRequest(withRegistryHTTPSOnly(ctx), http.MethodGet, listURL)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body := &limitedBody{r: resp.Body, n: limit}
	m, err := hostwatch.MatchThreatList(body, hosts)
	platform.NoteResponseSize(registryLoggerFrom(ctx), "threat-list", listURL, limit-body.n)
	if body.cut {
		return nil, fmt.Errorf("%w (%d bytes): %s", errThreatListTooLong, limit, platform.RedactURLUserinfo(listURL))
	}
	return m, err
}
