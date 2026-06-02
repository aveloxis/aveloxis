// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"mime"
	"net/mail"
	"strings"
	"time"
)

// ErrRateLimited signals an HTTP 429 from an archive source. Per design §8
// it feeds the Pacer (the source is healthy, just throttling) — NOT the
// circuit breaker. ErrTransient (5xx / transport) feeds the breaker.
var (
	ErrRateLimited = errors.New("archive source rate-limited (429)")
	ErrTransient   = errors.New("archive source transient error")
)

// ArchiveMessage is one parsed message from an archive, before
// classification. Backend-agnostic.
type ArchiveMessage struct {
	MessageID   string // RFC-822 Message-ID, angle brackets stripped
	ListID      string // List-Id header value
	ListAddress string // the list this came from, e.g. dev@kafka.apache.org
	Subject     string
	Sender      string // From header
	SenderEmail string // parsed address from From
	SentAt      time.Time
	InReplyTo   string
	References  string
	Body        string
	HasPatch    bool
}

// ArchiveSource reads a mailing-list archive. Implementations are pure
// fetch+parse: the worker applies the Pacer/Breaker defensive logic around
// these calls based on the returned error (ErrRateLimited vs ErrTransient).
type ArchiveSource interface {
	// Name is the system definition name (e.g. "apache_ponymail").
	Name() string
	// FetchMonth returns every message on listAddress in the yyyy-mm window.
	// retryAfter is the server's Retry-After (0 if absent), meaningful when
	// err wraps ErrRateLimited.
	FetchMonth(ctx context.Context, listAddress, yyyymm string) (msgs []ArchiveMessage, retryAfter time.Duration, err error)
}

// parseMbox splits an mboxrd byte stream into individual RFC-822 messages
// and parses each. Messages are separated by lines beginning with "From ".
// mboxrd ">From " body escaping is reversed. A message that fails to parse
// is skipped (best-effort, per the partial-success contract).
func parseMbox(data []byte, listAddress string) []ArchiveMessage {
	var out []ArchiveMessage
	for _, raw := range splitMbox(data) {
		am, ok := parseRFC822(raw, listAddress)
		if ok {
			out = append(out, am)
		}
	}
	return out
}

// splitMbox returns the raw RFC-822 body of each message (the "From " line
// itself is dropped). mboxrd ">From "/">>From " de-escaping is applied.
func splitMbox(data []byte) [][]byte {
	var msgs [][]byte
	var cur bytes.Buffer
	started := false
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	flush := func() {
		if started && cur.Len() > 0 {
			b := make([]byte, cur.Len())
			copy(b, cur.Bytes())
			msgs = append(msgs, b)
		}
		cur.Reset()
	}
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "From ") {
			flush()
			started = true
			continue
		}
		if !started {
			continue
		}
		// mboxrd de-escape: ">From " → "From ", ">>From " → ">From ", etc.
		if strings.HasPrefix(line, ">") && strings.HasPrefix(strings.TrimLeft(line, ">"), "From ") {
			line = line[1:]
		}
		cur.WriteString(line)
		cur.WriteByte('\n')
	}
	flush()
	return msgs
}

func parseRFC822(raw []byte, listAddress string) (ArchiveMessage, bool) {
	m, err := mail.ReadMessage(bytes.NewReader(raw))
	if err != nil {
		return ArchiveMessage{}, false
	}
	body, _ := io.ReadAll(m.Body)
	h := m.Header

	from := h.Get("From")
	senderEmail := ""
	if addr, err := mail.ParseAddress(from); err == nil {
		senderEmail = strings.ToLower(addr.Address)
	}
	var sentAt time.Time
	if d, err := h.Date(); err == nil {
		sentAt = d
	}

	am := ArchiveMessage{
		MessageID:   strings.Trim(h.Get("Message-Id"), "<> "),
		ListID:      h.Get("List-Id"),
		ListAddress: listAddress,
		Subject:     decodeHeader(h.Get("Subject")),
		Sender:      from,
		SenderEmail: senderEmail,
		SentAt:      sentAt,
		InReplyTo:   strings.Trim(h.Get("In-Reply-To"), "<> "),
		References:  h.Get("References"),
		Body:        string(body),
	}
	// .patch / .diff attachment is the kernel PR-equivalent signal.
	if ct := h.Get("Content-Type"); strings.Contains(ct, "multipart") &&
		(strings.Contains(string(body), "Content-Disposition: attachment") &&
			(strings.Contains(string(body), ".patch") || strings.Contains(string(body), ".diff"))) {
		am.HasPatch = true
	}
	return am, am.MessageID != ""
}

// decodeHeader decodes an RFC 2047 encoded-word header (e.g. =?UTF-8?...?=),
// falling back to the raw value on error.
func decodeHeader(v string) string {
	dec := new(mime.WordDecoder)
	if out, err := dec.DecodeHeader(v); err == nil {
		return out
	}
	return v
}
