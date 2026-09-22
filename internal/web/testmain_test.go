// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/testdb"
	"github.com/aveloxis/aveloxis/internal/testdb/prepare"
)

// TestMain runs this package's DB-tier tests in a database of their own,
// created from AVELOXIS_TEST_DB, prepared as an established deployment
// (migrated, bootstrap admin signed up) before any test, checked with the
// data-verify battery after green tests, and dropped (internal/testdb), so
// rows an earlier or concurrent run left behind can never reach them.
func TestMain(m *testing.M) { os.Exit(testdb.Main(m, prepare.Deployment, prepare.Verify)) }
