// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package stmttrace holds the store's one test seam on what it sends to
// the database.
//
// It is a package of its own, and holds a variable and nothing else, so
// that the seam costs the binaries one nil test and gives package db no
// exported function for setting it: the store reads Wrap when it builds a
// pool, and the DB-tier test harness (internal/testdb, through
// internal/testdb/writejudge) is the only code that gives it a value.
// scripts/statement_trace_seam_test.go reads the whole module for any
// other assignment, and for any non-test import of the judge.
package stmttrace

import "github.com/jackc/pgx/v5"

// Wrap is nil in every binary. When a test sets it, it is handed the tracer
// of every pool the store builds and returns the tracer the pool uses.
var Wrap func(pgx.QueryTracer) pgx.QueryTracer
