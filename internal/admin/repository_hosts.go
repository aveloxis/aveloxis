// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package admin

import (
	"context"
	"fmt"
	"strings"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/hostguard"
)

// The package repository hosts (v0.29.187; summary/70, 4.5, D14): which
// hosts poms have been fetched from, what depends on each, and the
// operator's decision not to use one. Administrator-only, here, at the
// deciding layer; every change is logged with who made it.
//
// Nothing in this file acts by itself (SR-7). A distrust is an explicit
// operation an administrator confirms after being shown what it will do.

// MaxRepositoryHostBytes bounds a host: a DNS name is at most 253 bytes, and
// a port adds at most six.
const MaxRepositoryHostBytes = 259

// MaxRepositoryHostNoteBytes bounds the reason an administrator gives.
const MaxRepositoryHostNoteBytes = 500

// RepositoryHostName is the one rule for a host an operation names, for an
// adapter that must carry the value on a page before any operation runs.
func RepositoryHostName(raw string) (string, error) { return repositoryHost(raw) }

// repositoryHost normalizes and checks a host an operation names.
func repositoryHost(raw string) (string, error) {
	host := hostguard.Normalize(raw)
	switch {
	case host == "":
		return "", refuseInput("host is required")
	case len(host) > MaxRepositoryHostBytes:
		return "", refuseInput(fmt.Sprintf("host is at most %d bytes", MaxRepositoryHostBytes))
	case strings.ContainsAny(host, " \t\r\n/\\@?#\"'<>&"):
		return "", refuseInput("host is a host name, with its port when it has one: no scheme, path, user or space")
	}
	return host, nil
}

// ListRepositoryHosts lists every known package repository host with what
// depends on it. configured are the hosts of the configured repositories,
// listed whether or not they have ever answered.
func (s *Service) ListRepositoryHosts(ctx context.Context, a Actor, configured []string) ([]db.RepositoryHost, error) {
	const op = "list repository hosts"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return nil, err
	}
	hosts, err := s.st.ListRepositoryHosts(ctx, configured)
	if err != nil {
		return nil, fmt.Errorf("%s by %s: %w", op, a, err)
	}
	return hosts, nil
}

// PreviewRepositoryHostDistrust says what distrusting host would do, and
// does nothing: it is what the operator is shown before the separate
// control that confirms.
func (s *Service) PreviewRepositoryHostDistrust(ctx context.Context, a Actor, rawHost string) (string, db.RepositoryHostImpact, error) {
	const op = "preview repository host distrust"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return "", db.RepositoryHostImpact{}, err
	}
	host, err := repositoryHost(rawHost)
	if err != nil {
		return "", db.RepositoryHostImpact{}, err
	}
	im, err := s.st.PreviewRepositoryHostDistrust(ctx, host)
	if err != nil {
		return host, im, fmt.Errorf("%s %s by %s: %w", op, host, a, err)
	}
	return host, im, nil
}

// DistrustRepositoryHost puts host on the distrusted list and deletes what
// is stored from it. From then on nothing is fetched from it and nothing it
// served is used; each dependency whose version came through it is "version
// not known" at its repository's next analysis. changed is false when the
// host was already distrusted.
func (s *Service) DistrustRepositoryHost(ctx context.Context, a Actor, rawHost, note string) (im db.RepositoryHostImpact, changed bool, err error) {
	const op = "distrust repository host"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return im, false, err
	}
	host, err := repositoryHost(rawHost)
	if err != nil {
		return im, false, err
	}
	note = strings.TrimSpace(note)
	if len(note) > MaxRepositoryHostNoteBytes {
		return im, false, refuseInput(fmt.Sprintf("note is at most %d bytes", MaxRepositoryHostNoteBytes))
	}
	im, changed, err = s.st.DistrustAndPurgeRepositoryHost(ctx, host, a.String(), note)
	if err != nil {
		return im, false, fmt.Errorf("%s %s by %s: %w", op, host, a, err)
	}
	// What the line says was DONE. Already distrusted, nothing was: the
	// impact is then only what is stored, and is not reported as an effect.
	done := im
	if !changed {
		done = db.RepositoryHostImpact{}
	}
	s.audit(op, a, "host", host, "changed", changed, "stored_results_deleted", done.StoredResults,
		"repositories", done.Repositories, "findings_to_close_as_version_lost", done.Findings, "trees_no_longer_whole", done.Trees)
	return im, changed, nil
}

// RestoreRepositoryHost takes host off the distrusted list. It is then
// fetched from again like any other, and versions resolved through it return
// at each repository's next analysis. changed is false when it was not
// distrusted.
func (s *Service) RestoreRepositoryHost(ctx context.Context, a Actor, rawHost string) (changed bool, err error) {
	const op = "restore repository host"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return false, err
	}
	host, err := repositoryHost(rawHost)
	if err != nil {
		return false, err
	}
	changed, err = s.st.RestoreRepositoryHost(ctx, host)
	if err != nil {
		return false, fmt.Errorf("%s %s by %s: %w", op, host, a, err)
	}
	s.audit(op, a, "host", host, "changed", changed)
	return changed, nil
}

// DismissRepositoryHostSignals marks the signals the host has now as seen:
// they leave the attention list, and a signal that begins later returns to
// it. It changes nothing else: the host is used exactly as before. n is the
// number of signals dismissed.
func (s *Service) DismissRepositoryHostSignals(ctx context.Context, a Actor, rawHost string) (n int, err error) {
	const op = "dismiss repository host signals"
	if err := s.requireAdmin(ctx, a, op); err != nil {
		return 0, err
	}
	host, err := repositoryHost(rawHost)
	if err != nil {
		return 0, err
	}
	n, err = s.st.DismissRepositoryHostSignals(ctx, host, a.String())
	if err != nil {
		return 0, fmt.Errorf("%s %s by %s: %w", op, host, a, err)
	}
	s.audit(op, a, "host", host, "signals_dismissed", n)
	return n, nil
}
