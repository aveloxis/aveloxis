// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Command gen-package-exposure-sql prints the two supply-chain matview
// definitions from the SQL the API shares with them (v0.29.60), for
// pasting into internal/db/matviews.sql. The parity test fails when the
// file and the code drift.
package main

import (
	"fmt"

	"github.com/aveloxis/aveloxis/internal/db"
)

func main() {
	fmt.Println("-- ---------------------------------------------------------------------------")
	fmt.Println("-- 23. explorer_package_exposure  --  one row per (ecosystem, package): the")
	fmt.Println("--     supply-chain cohort profile (v0.29.60). The body is")
	fmt.Println("--     db.PackageExposureMatviewSQL() verbatim — the same SQL the API runs")
	fmt.Println("--     live for a cohort with a repo filter; TestPackageExposureMatviewMatchesTheLiveSQL")
	fmt.Println("--     pins the two together. Regenerate with `go run ./scripts/gen-package-exposure-sql`.")
	fmt.Println("-- ---------------------------------------------------------------------------")
	fmt.Println("DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_exposure CASCADE;")
	fmt.Println("CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_package_exposure AS" + db.PackageExposureMatviewSQL() + ";")
	fmt.Println("CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_package_exposure_pkg ON aveloxis_data.explorer_package_exposure (ecosystem, package_name);")
	fmt.Println()
	fmt.Println("-- ---------------------------------------------------------------------------")
	fmt.Println("-- 24. explorer_package_advisory  --  one row per (ecosystem, package, advisory)")
	fmt.Println("--     (v0.29.60). Body: db.PackageAdvisoryMatviewSQL() verbatim.")
	fmt.Println("-- ---------------------------------------------------------------------------")
	fmt.Println("DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.explorer_package_advisory CASCADE;")
	fmt.Println("CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.explorer_package_advisory AS" + db.PackageAdvisoryMatviewSQL() + ";")
	fmt.Println("CREATE UNIQUE INDEX IF NOT EXISTS idx_explorer_package_advisory_pkg ON aveloxis_data.explorer_package_advisory (ecosystem, package_name, vuln_id);")
}
