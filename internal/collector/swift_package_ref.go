// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "strings"

// swiftPackageRef is ONE parse of a SwiftPM package repository URL — the
// normalizer every Swift site shares (SR-17; v0.29.59 review round 1: the
// Package.resolved reader, the Package.swift name extractor and the
// libyear resolver each had an inline parser, and they disagreed on
// subgroups, userinfo and ports, which broke the "direct and transitive
// are one purl" invariant this release exists to hold).
//
//	Host      lowercased, userinfo and port stripped ("github.com")
//	Owner     the first path segment ("Alamofire")
//	Repo      the LAST path segment, ".git" trimmed, case as spelled ("Alamofire")
//	Namespace host plus every segment before the repo ("github.com/Alamofire";
//	          "gitlab.com/group/sub" for a subgroup) — the purl namespace
type swiftPackageRef struct {
	Host, Owner, Repo, Namespace string
}

// parseSwiftPackageURL accepts https://, http://, ssh:// and git://
// URLs and scp-style git@host:owner/repo. A value without a dotted host
// and at least two path segments (a local path, a bare name) is not a
// package reference and returns ok=false.
func parseSwiftPackageURL(raw string) (ref swiftPackageRef, ok bool) {
	u := strings.TrimSpace(raw)
	switch {
	case strings.Contains(u, "://"):
		u = u[strings.Index(u, "://")+3:]
	case strings.Contains(u, "@") && strings.Contains(u[strings.Index(u, "@"):], ":"):
		// scp-style: git@github.com:owner/repo.git
		u = strings.Replace(u[strings.Index(u, "@")+1:], ":", "/", 1)
	default:
		return swiftPackageRef{}, false
	}
	slash := strings.IndexByte(u, '/')
	if slash <= 0 {
		return swiftPackageRef{}, false
	}
	host, path := u[:slash], u[slash+1:]
	if at := strings.LastIndexByte(host, '@'); at >= 0 {
		host = host[at+1:] // userinfo
	}
	if colon := strings.IndexByte(host, ':'); colon >= 0 {
		host = host[:colon] // port
	}
	host = strings.ToLower(host)
	if host == "" || !strings.Contains(host, ".") {
		return swiftPackageRef{}, false
	}
	path = strings.TrimSuffix(strings.TrimSuffix(strings.Trim(path, "/"), ".git"), "/")
	segs := strings.Split(path, "/")
	if len(segs) < 2 {
		return swiftPackageRef{}, false
	}
	for _, s := range segs {
		if s == "" {
			return swiftPackageRef{}, false
		}
	}
	repo := segs[len(segs)-1]
	return swiftPackageRef{
		Host:      host,
		Owner:     segs[0],
		Repo:      repo,
		Namespace: host + "/" + strings.Join(segs[:len(segs)-1], "/"),
	}, true
}
