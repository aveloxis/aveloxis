// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/aveloxis/aveloxis/internal/db"
)

// The runbook's offline half, end to end (docs/guide/signup-escrow.md):
// keygen writes the private key (never over an existing one) and prints the
// public key; an envelope sealed to that public key, in export's file
// format, opens with the key file — and the wrong key is an error.
func TestSignupEscrowKeygenAndOpen(t *testing.T) {
	dir := t.TempDir()
	keyFile := filepath.Join(dir, "escrow-key.txt")
	var out bytes.Buffer
	kg := signupEscrowKeygenCmd()
	kg.SetOut(&out)
	kg.SetArgs([]string{"--out", keyFile})
	if err := kg.Execute(); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(keyFile); err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file = %v, %v; want mode 600", fi, err)
	}
	kg2 := signupEscrowKeygenCmd()
	kg2.SetOut(&bytes.Buffer{})
	kg2.SetArgs([]string{"--out", keyFile})
	if err := kg2.Execute(); err == nil {
		t.Fatal("keygen overwrote an existing key")
	}
	pub := ""
	for _, line := range strings.Split(out.String(), "\n") {
		if i := strings.Index(line, `"age1`); i >= 0 {
			pub = strings.Trim(line[i:], `" `)
		}
	}
	recipient, err := db.ParseSignupEscrowRecipient(pub)
	if err != nil {
		t.Fatalf("keygen printed no usable public key: %q (%v)", pub, err)
	}
	sealed, err := db.SealSignupAddress(recipient, netip.MustParseAddr("203.0.113.77"))
	if err != nil {
		t.Fatal(err)
	}
	export := filepath.Join(dir, "export.json")
	b, _ := json.Marshal([]db.SealedSignup{{SignupID: 9, UserID: 42, Login: "someone", CreatedAt: time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC), Sealed: sealed}})
	if err := os.WriteFile(export, b, 0o600); err != nil {
		t.Fatal(err)
	}
	var opened bytes.Buffer
	op := signupEscrowOpenCmd()
	op.SetOut(&opened)
	op.SetArgs([]string{"--identity", keyFile, export})
	if err := op.Execute(); err != nil {
		t.Fatalf("open = %v\n%s", err, opened.String())
	}
	if !strings.Contains(opened.String(), "42\tsomeone\t2026-10-10 12:00:00\t203.0.113.77") {
		t.Fatalf("open printed:\n%s", opened.String())
	}
	// A key file holding an old key and the new one opens envelopes sealed
	// to either (closing review r3 F11: only the first key was tried).
	both := filepath.Join(dir, "both.txt")
	oldKey, _ := age.GenerateX25519Identity()
	keyBody, _ := os.ReadFile(keyFile)
	_ = os.WriteFile(both, append([]byte(oldKey.String()+"\n"), keyBody...), 0o600)
	op3 := signupEscrowOpenCmd()
	op3.SetOut(&bytes.Buffer{})
	op3.SetArgs([]string{"--identity", both, export})
	if err := op3.Execute(); err != nil {
		t.Errorf("a key file with the old key first did not open the envelope: %v", err)
	}
	other, _ := age.GenerateX25519Identity()
	wrong := filepath.Join(dir, "wrong.txt")
	_ = os.WriteFile(wrong, []byte(other.String()+"\n"), 0o600)
	op2 := signupEscrowOpenCmd()
	op2.SetOut(&bytes.Buffer{})
	op2.SetArgs([]string{"--identity", wrong, export})
	if err := op2.Execute(); err == nil {
		t.Fatal("the wrong key opened the envelope")
	}
}
