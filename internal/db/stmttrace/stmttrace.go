// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package stmttrace holds the one test seam on what this module sends to
// the database.
//
// It is a package of its own, and holds a variable and a marker and
// nothing else, so that the seam costs the binaries one nil test and gives
// package db no exported function for setting it: internal/db's openers
// (open.go: every pool and connection of the module is opened there) read
// Wrap, and the DB-tier test harness (internal/testdb, through
// internal/testdb/writejudge) is the only code that gives it a value.
// scripts/statement_trace_seam_test.go reads the whole module for any
// other assignment, and for any non-test import of the judge.
package stmttrace

import "github.com/jackc/pgx/v5"

// Wrap is nil in every binary. When a test sets it, it is handed the tracer
// of every pool and connection the module opens (internal/db's openers) and
// returns the tracer that pool or connection uses.
var Wrap func(pgx.QueryTracer) pgx.QueryTracer

// Seamed marks a tracer Wrap returned: the openers ask it of the tracer a
// connection is about to be made with, and put the seam back when
// something took it off (a pool's BeforeConnect hook may replace the
// tracer of the configuration it is handed).
type Seamed interface{ StatementSeam() }
