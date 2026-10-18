// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package model

import "time"

// Commit represents a single commit's changes to one file.
// Multiple Commit rows may share the same Hash (one per file touched).
type Commit struct {
	ID                   int64
	RepoID               int64
	Hash                 string
	AuthorName           string
	AuthorRawEmail       string
	AuthorEmail          string // resolved
	AuthorDate           string // YYYY-MM-DD
	AuthorAffiliation    string
	AuthorTimestamp      *time.Time
	AuthorPlatformLogin  string // cntrb_login for FK resolution
	CommitterName        string
	CommitterRawEmail    string
	CommitterEmail       string
	CommitterDate        string
	CommitterAffiliation string
	CommitterTimestamp   *time.Time
	Filename             string
	LinesAdded           int
	LinesRemoved         int
	LinesWhitespace      int
	Origin               DataOrigin
}

// CommitMessage holds the commit message text separately from per-file rows.
type CommitMessage struct {
	RepoID  int64
	Hash    string
	Message string
}
