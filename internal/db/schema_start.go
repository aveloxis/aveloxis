// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
)

// ErrSchemaBehind is the refusal for a schema stamp behind this binary —
// its migration has not run against this database.
var ErrSchemaBehind = errors.New("the database schema is behind this binary")

// ErrSchemaUnknown is the refusal for a database with no schema stamp —
// `aveloxis migrate` has never run against it.
var ErrSchemaUnknown = errors.New("the database schema is unstamped")

// RequireSchemaCurrent is the start gate of every process that never
// migrates — web, api, the scancode worker (worklist item 49). Through
// v0.29.67 they logged an ERROR when the stamp was behind their binary and
// then served queries against columns the schema did not have (kate,
// 2026-09-13 and 2026-09-22: about 80 minutes of "does not exist" errors
// while the migrate ran). Now they refuse, before binding a port, with the
// migrate named. A stamp AHEAD of the binary (an older binary against a
// newer schema, the rollback case) proceeds; a stamp that could not be read
// refuses with the read error (SR-5). There is no bypass flag: the migrate
// is minutes since v0.29.62, and a process serving a schema it does not
// know is the outage this prevents.
func (s *PostgresStore) RequireSchemaCurrent(ctx context.Context) error {
	stamp, err := s.schemaVersionProbe(ctx)
	return schemaStartRefusal(stamp, err)
}

// schemaStartRefusal is RequireSchemaCurrent's decision, split out so it is
// testable without a database (TestSchemaStartRefusal).
func schemaStartRefusal(stamp string, readErr error) error {
	if readErr != nil {
		return fmt.Errorf("reading the schema stamp: %w", readErr)
	}
	if stamp == "" {
		return fmt.Errorf("%w — `aveloxis migrate` has never run against this database; run %s, then start this process (it never migrates)", ErrSchemaUnknown, DeployStepsAdvice)
	}
	if !SchemaVersionAtLeast(stamp, ToolVersion) {
		return fmt.Errorf("%w (schema %s, binary %s) — run %s, then start this process (it never migrates)", ErrSchemaBehind, stamp, ToolVersion, DeployStepsAdvice)
	}
	return nil
}
