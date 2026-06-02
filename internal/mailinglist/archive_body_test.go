// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailinglist

import (
	"encoding/base64"
	"net/textproto"
	"strings"
	"testing"
)

func TestDecodeTransferQuotedPrintable(t *testing.T) {
	got := string(decodeTransfer([]byte("Hello=20World=3D=0Anext"), "quoted-printable"))
	if got != "Hello World=\nnext" {
		t.Errorf("quoted-printable decode = %q", got)
	}
}

func TestDecodeTransferBase64(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString([]byte("Reviewed-by: Bjorn"))
	// base64 bodies are line-wrapped in real mail; ensure whitespace is tolerated.
	wrapped := enc[:4] + "\n" + enc[4:]
	got := string(decodeTransfer([]byte(wrapped), "base64"))
	if got != "Reviewed-by: Bjorn" {
		t.Errorf("base64 decode = %q", got)
	}
}

func TestExtractBodyMultipartPrefersTextPlain(t *testing.T) {
	raw := "--BOUND\r\n" +
		"Content-Type: text/plain\r\n\r\n" +
		"the human text\r\n" +
		"--BOUND\r\n" +
		"Content-Type: text/html\r\n\r\n" +
		"<p>ignore me</p>\r\n" +
		"--BOUND--\r\n"
	h := textproto.MIMEHeader{"Content-Type": {`multipart/alternative; boundary="BOUND"`}}
	body, _ := extractBody(h, []byte(raw))
	if !strings.Contains(body, "the human text") || strings.Contains(body, "ignore me") {
		t.Errorf("multipart body = %q (want text/plain part only)", body)
	}
}

func TestExtractBodyDetectsInlinePatchAndAttachment(t *testing.T) {
	// Inline git diff (kernel style).
	h := textproto.MIMEHeader{}
	_, patch := extractBody(h, []byte("Subject body\ndiff --git a/f b/f\n@@ -1 +1 @@\n"))
	if !patch {
		t.Error("inline 'diff --git' must set has_patch")
	}
	// .patch attachment in multipart.
	raw := "--B\r\nContent-Type: text/plain\r\n\r\nsee attached\r\n" +
		"--B\r\nContent-Type: application/octet-stream\r\n" +
		"Content-Disposition: attachment; filename=\"fix.patch\"\r\n\r\ndata\r\n--B--\r\n"
	hm := textproto.MIMEHeader{"Content-Type": {`multipart/mixed; boundary="B"`}}
	_, patch2 := extractBody(hm, []byte(raw))
	if !patch2 {
		t.Error(".patch attachment must set has_patch")
	}
}

func TestLooksLikePatchNegative(t *testing.T) {
	if looksLikePatch("just a normal discussion reply with no diff") {
		t.Error("plain text must not be flagged as a patch")
	}
}
