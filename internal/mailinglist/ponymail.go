// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// PonyMail is the apache_ponymail ArchiveSource backend. lists.apache.org
// runs the Foal suite; its mbox.lua endpoint returns a whole list-month as
// an mboxrd stream in one request — the efficient bulk-backfill path (~one
// request per list-month vs tens of thousands per message).
type PonyMail struct {
	baseURL   string
	userAgent string
	client    *http.Client
}

// DefaultUserAgent identifies the collector politely so archive admins can
// reach us instead of blocking (design §8). Operators override the contact
// via config.
const DefaultUserAgent = "Aveloxis/0.25.7 (+https://github.com/aveloxis/aveloxis)"

// NewPonyMail builds the backend. baseURL defaults to the public host.
func NewPonyMail(baseURL, userAgent string) *PonyMail {
	if baseURL == "" {
		baseURL = "https://lists.apache.org"
	}
	if userAgent == "" {
		userAgent = DefaultUserAgent
	}
	return &PonyMail{
		baseURL:   strings.TrimRight(baseURL, "/"),
		userAgent: userAgent,
		client:    &http.Client{Timeout: 90 * time.Second},
	}
}

func (p *PonyMail) Name() string { return "apache_ponymail" }

// FetchMonth pulls one list-month via mbox.lua and parses the mboxrd stream.
// A 404 (empty month) is a clean zero-result, not an error. 429 → ErrRateLimited
// (+ Retry-After); 5xx / transport → ErrTransient — the worker routes these
// to the Pacer / Breaker respectively.
func (p *PonyMail) FetchMonth(ctx context.Context, listAddress, yyyymm string) ([]ArchiveMessage, time.Duration, error) {
	list, domain := splitListAddress(listAddress)
	if list == "" || domain == "" {
		return nil, 0, fmt.Errorf("invalid list address %q", listAddress)
	}
	u := fmt.Sprintf("%s/api/mbox.lua?list=%s&domain=%s&date=%s",
		p.baseURL, url.QueryEscape(list), url.QueryEscape(domain), url.QueryEscape(yyyymm))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", p.userAgent)

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("mbox.lua %s: %w", listAddress, ErrTransient) // transport error
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusOK:
		body, rerr := io.ReadAll(resp.Body)
		if rerr != nil {
			return nil, 0, fmt.Errorf("read mbox %s: %w", listAddress, ErrTransient)
		}
		return parseMbox(body, listAddress), 0, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, parseRetryAfter(resp.Header.Get("Retry-After")), fmt.Errorf("mbox.lua %s: %w", listAddress, ErrRateLimited)
	case resp.StatusCode == http.StatusNotFound:
		return nil, 0, nil // empty month — clean zero result
	case resp.StatusCode >= 500:
		return nil, parseRetryAfter(resp.Header.Get("Retry-After")), fmt.Errorf("mbox.lua %s: status %d: %w", listAddress, resp.StatusCode, ErrTransient)
	default:
		return nil, 0, fmt.Errorf("mbox.lua %s: unexpected status %d", listAddress, resp.StatusCode)
	}
}

// splitListAddress turns "dev@kafka.apache.org" into ("dev","kafka.apache.org").
func splitListAddress(addr string) (list, domain string) {
	at := strings.IndexByte(addr, '@')
	if at <= 0 || at == len(addr)-1 {
		return "", ""
	}
	return addr[:at], addr[at+1:]
}

// parseRetryAfter reads a Retry-After header expressed in seconds. Returns 0
// when absent or unparseable (the Pacer's own backoff then governs).
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil && secs >= 0 {
		return time.Duration(secs) * time.Second
	}
	return 0
}
