// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// digestWindow decides whether a digest is due and what window it
// covers. Pure so the semantics are unit-testable:
//   - first run (last is zero): due immediately, but the window opens
//     only one interval back — NOT the epoch, so enabling the feature
//     on an established fleet doesn't dump the entire historical
//     findings table into one email.
//   - otherwise: due when >= interval has elapsed; the window opens at
//     the previous stamp so nothing between stamps is skipped.
func digestWindow(now, last time.Time, interval time.Duration) (since time.Time, due bool) {
	if last.IsZero() {
		return now.Add(-interval), true
	}
	return last, now.Sub(last) >= interval
}

// runVulnDigest is the v0.27.12 operator-notification pass: findings
// first detected since the previous digest, unresolved, at or above
// the configured severity floor, emailed to mail.operator_email.
//
// Stamp semantics: the stamp advances after EVERY evaluated window —
// including quiet ones (no findings → no email, window still moves) —
// but NOT after a failed send, so an SMTP outage retries the same
// window on the next tick instead of dropping findings.
func (s *Scheduler) runVulnDigest(ctx context.Context) {
	if s.digestMailer == nil || s.cfg.Mail == nil || s.cfg.Mail.OperatorEmail == "" {
		return
	}
	stampPath := s.digestStampPath
	if stampPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			s.logger.Warn("vuln digest: cannot resolve home dir for stamp file", "error", err)
			return
		}
		stampPath = filepath.Join(home, ".aveloxis", "vuln-digest-last")
	}

	now := time.Now()
	since, due := digestWindow(now, readDigestStamp(stampPath), s.cfg.Mail.VulnDigestInterval())
	if !due {
		return
	}

	items, err := s.store.GetNewVulnerabilityFindings(ctx, since,
		s.cfg.Mail.VulnDigestMinSeverityOrDefault(), s.cfg.Mail.VulnDigestIncludeTransitive,
		s.cfg.Mail.VulnDigestIncludeDev)
	if err != nil {
		s.logger.Warn("vuln digest: query failed", "error", err)
		return
	}
	if len(items) > 0 {
		if err := s.digestMailer.SendVulnerabilityDigest(s.cfg.Mail.OperatorEmail, since, items); err != nil {
			// No stamp: retry this same window on the next tick.
			s.logger.Warn("vuln digest: send failed — window will retry", "error", err, "findings", len(items))
			return
		}
		s.logger.Info("vuln digest sent", "to", s.cfg.Mail.OperatorEmail,
			"findings", len(items), "window_since", since)
	}
	if err := writeDigestStamp(stampPath, now); err != nil {
		s.logger.Warn("vuln digest: stamp write failed", "path", stampPath, "error", err)
	}
}

// readDigestStamp returns the last digest time, or zero when the stamp
// file is absent/unreadable/garbled (first run semantics apply).
func readDigestStamp(path string) time.Time {
	b, err := os.ReadFile(path)
	if err != nil {
		return time.Time{}
	}
	sec, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(sec, 0)
}

func writeDigestStamp(path string, t time.Time) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(strconv.FormatInt(t.Unix(), 10)), 0o644)
}
