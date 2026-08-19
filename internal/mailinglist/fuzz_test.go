// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"testing"
)

// FuzzParseMbox drives the full archive-ingestion parse chain on hostile
// mbox bytes: splitMbox framing → parseRFC822 headers → MIME multipart
// extraction → transfer-encoding decode → RFC 2047 header decode — and
// then classifies every parsed message the way the worker does. Email is
// a legendarily hostile format (broken boundaries, malformed
// encoded-words, 8-bit-as-7-bit, mboxrd "From " escaping); the archive
// side of this chain ingests whatever Pony Mail / public-inbox serves.
//
// Contract fuzzed: never panic, never hang. Invalid UTF-8 in parsed
// fields is TOLERATED by design — the v0.23.5 pgx boundary tracer scrubs
// string params at the wire, so the parser is not required to emit clean
// UTF-8 (do not "strengthen" this into a UTF-8 assertion; it would flag
// designed behavior).
//
// Runs under ClusterFuzzLite (fuzz_mailinglist_mbox) and, seeds-only,
// in every ordinary `go test` run.
func FuzzParseMbox(f *testing.F) {
	systems, err := LoadSystems()
	if err != nil {
		f.Fatalf("LoadSystems: %v", err)
	}
	system := systems["apache_ponymail"]
	if system == nil {
		f.Fatal("apache_ponymail system missing from embedded systems.yaml")
	}

	// Valid message.
	f.Add([]byte("From alice@example.org Mon Jan  1 00:00:00 2026\n" +
		"From: =?utf-8?q?Alice?= <alice@example.org>\n" +
		"Message-ID: <m1@example.org>\n" +
		"List-Id: <dev.kafka.apache.org>\n" +
		"Subject: [VOTE] release candidate\n" +
		"Date: Mon, 1 Jan 2026 00:00:00 +0000\n" +
		"Content-Type: text/plain; charset=utf-8\n\n" +
		"+1 (binding)\n>From escaped body line\n"))
	// Multipart with a broken boundary.
	f.Add([]byte("From b@x Mon Jan  1 00:00:00 2026\n" +
		"From: b@x\nSubject: multi\n" +
		"Content-Type: multipart/alternative; boundary=\"b1\"\n\n" +
		"--b1\nContent-Type: text/plain\n\nplain part\n--b1\nContent-Type: text/html\n\n<b>html</b>\n--b1--\n" +
		"--b1\nno terminator"))
	// Quoted-printable + base64 transfer encodings, hostile encoded-word.
	f.Add([]byte("From c@x Mon Jan  1 00:00:00 2026\n" +
		"From: =?bogus-charset?B?////?= <c@x>\n" +
		"Content-Transfer-Encoding: quoted-printable\n\n=E2=82=AC=XX=\n"))
	f.Add([]byte("From d@x Mon Jan  1 00:00:00 2026\nFrom: d@x\n" +
		"Content-Transfer-Encoding: base64\n\nnot!!valid$$base64==\n"))
	// Pathological framing: NULs, no headers, bare "From " lines.
	f.Add([]byte("From \x00\nFrom \nFrom x\n\n\x00\xff\xfe"))
	f.Add([]byte(""))

	f.Fuzz(func(t *testing.T, data []byte) {
		msgs := parseMbox(data, "dev@kafka.apache.org")
		for _, m := range msgs {
			// The worker's next step: classification must hold up
			// against anything the parser emits.
			_ = system.Classify(Message{
				ListID:      m.ListID,
				ListAddress: m.ListAddress,
				Subject:     m.Subject,
				Sender:      m.Sender,
				Body:        m.Body,
			})
		}
	})
}

// FuzzDecodeHeader isolates the RFC 2047 encoded-word decoder — the
// classic per-header panic site (charset tricks, truncated encodings).
func FuzzDecodeHeader(f *testing.F) {
	f.Add("=?utf-8?q?Alice_Smith?=")
	f.Add("=?UTF-8?B?QWxpY2U=?=")
	f.Add("=?bogus?X?$$?=")
	f.Add("plain header")
	f.Fuzz(func(t *testing.T, v string) {
		_ = decodeHeader(v)
	})
}
