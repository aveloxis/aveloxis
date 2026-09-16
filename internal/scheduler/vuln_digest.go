// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
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
// but NOT after a failed send (a mailer skip included), so an SMTP outage
// retries the same window on the next tick instead of dropping findings;
// holdDigestWindow covers a failure before any stamp exists.
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
	last := readDigestStamp(stampPath)
	since, due := digestWindow(now, last, s.cfg.Mail.VulnDigestInterval())
	if !due {
		return
	}

	items, err := s.store.GetNewVulnerabilityFindings(ctx, since,
		s.cfg.Mail.VulnDigestMinSeverityOrDefault(), s.cfg.Mail.VulnDigestIncludeTransitive,
		s.cfg.Mail.VulnDigestIncludeDev)
	if errors.Is(err, context.Canceled) {
		return // shutdown, not a failure: the stamp is untouched, the window retries next hour
	}
	if err != nil {
		s.logger.Warn("vuln digest: query failed", "error", err)
		return
	}
	if len(items) > 0 {
		if err := s.digestMailer.SendVulnerabilityDigest(s.cfg.Mail.OperatorEmail, since, items); err != nil {
			// Retry this same window on the next tick. A mailer skip
			// is a failure here too: nothing was delivered.
			s.logger.Warn("vuln digest: send failed — window will retry", "error", err, "findings", len(items))
			if holdErr := holdDigestWindow(stampPath, last, since); holdErr != nil {
				s.logger.Warn("vuln digest: could not pin the retry window", "path", stampPath, "error", holdErr)
			}
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

// vulnDigestReady decides whether Run starts the digest ticker. It returns
// false and no error when the digest is simply not configured (no mailer
// injected, or no operator_email), and false with the reason when it is
// configured but could never deliver — a disabled mailer or an
// operator_email that is not one deliverable address. Before v0.29.29 that
// case logged "operator vulnerability digest enabled" and then ran the
// findings query every hour without sending. The mailer is built once at
// startup, so a fixed config takes effect on restart.
func (s *Scheduler) vulnDigestReady() (bool, error) {
	if !(s.digestMailer != nil && s.cfg.Mail != nil && s.cfg.Mail.OperatorEmail != "") {
		return false, nil
	}
	if err := s.digestMailer.Deliverable(s.cfg.Mail.OperatorEmail); err != nil {
		return false, err
	}
	return true, nil
}

// holdDigestWindow keeps a failed send's window for the next tick. With a
// stamp, leaving it untouched does that. Without one (first run),
// digestWindow opens the window one interval back from NOW, so every failed
// tick would slide it forward and drop its oldest findings; writing `since`
// pins it instead.
func holdDigestWindow(stampPath string, last, since time.Time) error {
	if !last.IsZero() {
		return nil
	}
	return writeDigestStamp(stampPath, since)
}

// startVulnDigest starts the hourly digest check when vulnDigestReady allows
// it, and says so in the log either way: ERROR once when a configured digest
// could never be delivered, INFO when it starts, nothing when it is not
// configured. It returns the tick channel (nil = disabled) and the stop
// function Run defers.
func (s *Scheduler) startVulnDigest() (<-chan time.Time, func()) {
	ready, err := s.vulnDigestReady()
	if err != nil {
		s.logger.Error("operator vulnerability digest NOT started: no digest could be delivered — fix the mail block or mail.operator_email, then restart serve",
			"operator_email", s.cfg.Mail.OperatorEmail, "error", err)
	}
	if !ready {
		return nil, func() {}
	}
	ticker := time.NewTicker(1 * time.Hour)
	s.logger.Info("operator vulnerability digest enabled",
		"operator_email", s.cfg.Mail.OperatorEmail,
		"min_severity", s.cfg.Mail.VulnDigestMinSeverityOrDefault(),
		"interval", s.cfg.Mail.VulnDigestInterval())
	return ticker.C, ticker.Stop
}
