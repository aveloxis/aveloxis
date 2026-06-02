// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/mail"
	"net/textproto"
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

// ListInfo describes one list discovered by enumeration.
type ListInfo struct {
	Name    string // the list local-part, e.g. "dev" or "common-dev"
	Address string // full address, e.g. "dev@kafka.apache.org"
	Count   int    // archived message count (best-effort)
}

// ArchiveSource reads a mailing-list archive. Implementations are pure
// fetch+parse: the worker applies the Pacer/Breaker defensive logic around
// these calls based on the returned error (ErrRateLimited vs ErrTransient).
type ArchiveSource interface {
	// Name is the system definition name (e.g. "apache_ponymail").
	Name() string
	// EnumerateLists returns the lists that actually exist under a domain
	// (e.g. "kafka.apache.org"). Replaces hardcoded list-name guessing and
	// surfaces naming-drift lists (cvs@, common-dev@) automatically.
	EnumerateLists(ctx context.Context, domain string) ([]ListInfo, error)
	// FirstMonth returns the earliest yyyy-mm with traffic on a list ("" if
	// the list doesn't exist / has none). Used for full-history backfill.
	FirstMonth(ctx context.Context, listAddress string) (string, error)
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
	rawBody, _ := io.ReadAll(m.Body)
	h := m.Header
	body, hasPatch := extractBody(textproto.MIMEHeader(h), rawBody)

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
		Body:        body,
		HasPatch:    hasPatch,
	}
	return am, am.MessageID != ""
}

// extractBody returns the human-readable body text and a has-patch flag.
// It decodes Content-Transfer-Encoding (quoted-printable / base64) and, for
// multipart messages, extracts the first text/plain part — so body-based
// classification (github-mirror URLs, Reviewed-by trailers) and stored
// msg_text see clean text rather than MIME/encoded noise. has_patch is set
// when a part is a .patch/.diff attachment OR the text contains inline diff
// markers (the kernel emails patches inline, not as attachments).
func extractBody(h textproto.MIMEHeader, rawBody []byte) (string, bool) {
	mediaType, params, err := mime.ParseMediaType(h.Get("Content-Type"))
	if err == nil && strings.HasPrefix(mediaType, "multipart/") && params["boundary"] != "" {
		text, patch := extractMultipart(rawBody, params["boundary"])
		return text, patch
	}
	decoded := decodeTransfer(rawBody, h.Get("Content-Transfer-Encoding"))
	return string(decoded), looksLikePatch(string(decoded))
}

// extractMultipart walks a multipart body: first text/plain part becomes the
// body; a .patch/.diff attachment (or text/x-patch part) sets has_patch.
func extractMultipart(rawBody []byte, boundary string) (string, bool) {
	mr := multipart.NewReader(bytes.NewReader(rawBody), boundary)
	var text string
	hasPatch := false
	for {
		part, err := mr.NextPart()
		if err != nil {
			break
		}
		partBytes, _ := io.ReadAll(part)
		decoded := decodeTransfer(partBytes, part.Header.Get("Content-Transfer-Encoding"))
		pmt, _, _ := mime.ParseMediaType(part.Header.Get("Content-Type"))
		disp := part.Header.Get("Content-Disposition")
		fn := part.FileName()
		if strings.HasSuffix(fn, ".patch") || strings.HasSuffix(fn, ".diff") ||
			pmt == "text/x-patch" || pmt == "text/x-diff" {
			hasPatch = true
		}
		if text == "" && (pmt == "text/plain" || (pmt == "" && !strings.Contains(disp, "attachment"))) {
			text = string(decoded)
		}
		if looksLikePatch(string(decoded)) {
			hasPatch = true
		}
	}
	return text, hasPatch
}

// decodeTransfer reverses quoted-printable / base64 Content-Transfer-Encoding.
func decodeTransfer(b []byte, enc string) []byte {
	switch strings.ToLower(strings.TrimSpace(enc)) {
	case "quoted-printable":
		if out, err := io.ReadAll(quotedprintable.NewReader(bytes.NewReader(b))); err == nil {
			return out
		}
	case "base64":
		clean := strings.Map(func(r rune) rune {
			if r == '\n' || r == '\r' || r == ' ' || r == '\t' {
				return -1
			}
			return r
		}, string(b))
		if out, err := base64.StdEncoding.DecodeString(clean); err == nil {
			return out
		}
	}
	return b
}

// looksLikePatch detects an inline unified diff (git or plain).
func looksLikePatch(s string) bool {
	if strings.Contains(s, "diff --git ") {
		return true
	}
	return strings.Contains(s, "\n--- ") && strings.Contains(s, "\n+++ ")
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
