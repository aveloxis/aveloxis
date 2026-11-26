// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

// fixtureTables are the base tables the package's fixtures write. A write
// is accepted only on a base table the catalog knows, so a fixture says
// which exist; a name that is not here is unknown to the judge, as a name
// the schema does not create is to the store's own test.
var fixtureTables = []string{
	"aveloxis_data.repos", "aveloxis_data.other", "aveloxis_data.staging", "aveloxis_data.m",
	"aveloxis_ops.notes", "aveloxis_ops.queue", "aveloxis_data.messages", "aveloxis_data.repo_deps_vulnerabilities",
}

// fixtureCheck judges the statements with a catalog of what they define
// themselves and the fixture tables.
func fixtureCheck(stmts []Stmt, registry []Registered, exceptions []Exception) Report {
	return CheckIn(CatalogOf(stmts).Table(fixtureTables...), stmts, registry, exceptions)
}
