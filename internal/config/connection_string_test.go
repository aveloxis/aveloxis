// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestConnectionStringRoundTrips — old problem O8: ConnectionString
// formatted the password (and user and database) into the URL unescaped, so
// a password holding `@ / % ? # :` or a space broke every connection with a
// parse error that quoted the string. pgx's own parser is the oracle: what
// the config holds comes back unchanged.
func TestConnectionStringRoundTrips(t *testing.T) {
	for _, d := range []DatabaseConfig{
		{Host: "db.example", Port: 5432, User: "aveloxis", Password: "p@ss/w%rd?x#y:z w", DBName: "aveloxis_large", SSLMode: "disable"},
		{Host: "localhost", Port: 5433, User: "us@er", Password: "plain", DBName: "db name", SSLMode: "disable"},
		{Host: "::1", Port: 5432, User: "u", Password: "p", DBName: "d", SSLMode: "disable"},
		{Host: "/tmp", Port: 5432, User: "u", Password: "p", DBName: "d", SSLMode: "disable"},
	} {
		for name, dsn := range map[string]string{"plain": d.ConnectionString(), "app": d.ConnectionStringWithAppName("aveloxis-api@kate")} {
			pc, err := pgx.ParseConfig(dsn)
			if err != nil {
				t.Errorf("%s host=%q user=%q: does not parse (the error is not printed: it can quote the password)", name, d.Host, d.User)
				continue
			}
			if pc.Host != d.Host || int(pc.Port) != d.Port || pc.User != d.User || pc.Password != d.Password || pc.Database != d.DBName {
				t.Errorf("%s: round trip changed the config: host %q→%q port %d→%d user %q→%q db %q→%q password equal=%v",
					name, d.Host, pc.Host, d.Port, pc.Port, d.User, pc.User, d.DBName, pc.Database, pc.Password == d.Password)
			}
			if name == "app" && pc.RuntimeParams["application_name"] != "aveloxis-api@kate" {
				t.Errorf("application_name = %q", pc.RuntimeParams["application_name"])
			}
		}
	}
}

// TestConnectionStringAcceptsABracketedIPv6Host — review round 1 (minor):
// the pre-O8 DSN accepted "[::1]" (it was pasted into "@[::1]:5432");
// net.JoinHostPort added a second pair of brackets, which pgx refuses.
func TestConnectionStringAcceptsABracketedIPv6Host(t *testing.T) {
	d := DatabaseConfig{Host: "[::1]", Port: 5432, User: "u", Password: "p", DBName: "d", SSLMode: "disable"}
	pc, err := pgx.ParseConfig(d.ConnectionString())
	if err != nil {
		t.Fatalf("a bracketed IPv6 host does not parse: %v", err)
	}
	if pc.Host != "::1" || pc.Port != 5432 {
		t.Errorf("host %q port %d, want ::1 5432", pc.Host, pc.Port)
	}
}
