// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Command gen_spdx_data regenerates internal/spdx/spdx_data.tsv from the
// official SPDX license-list-data (licenses.json and exceptions.json). It is
// a developer tool, never run by aveloxis itself: the data ships inside the
// binary, and nothing fetches it at runtime.
//
// Usage:
//
//	go run ./scripts/gen_spdx_data > internal/spdx/spdx_data.tsv
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

const base = "https://raw.githubusercontent.com/spdx/license-list-data/main/json/"

type licenseList struct {
	Version  string `json:"licenseListVersion"`
	Released string `json:"releaseDate"`
	Licenses []struct {
		ID         string `json:"licenseId"`
		OSI        bool   `json:"isOsiApproved"`
		Deprecated bool   `json:"isDeprecatedLicenseId"`
	} `json:"licenses"`
}

type exceptionList struct {
	Exceptions []struct {
		ID         string `json:"licenseExceptionId"`
		Deprecated bool   `json:"isDeprecatedLicenseId"`
	} `json:"exceptions"`
}

func fetch(name string, into any) error {
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(base + name)
	if err != nil {
		return fmt.Errorf("fetch %s: %w", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch %s: HTTP %d", name, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read %s: %w", name, err)
	}
	if err := json.Unmarshal(body, into); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}

func bit(b bool) int {
	if b {
		return 1
	}
	return 0
}

func main() {
	var lic licenseList
	var exc exceptionList
	if err := fetch("licenses.json", &lic); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := fetch("exceptions.json", &exc); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if len(lic.Licenses) == 0 || len(exc.Exceptions) == 0 {
		fmt.Fprintln(os.Stderr, "empty license or exception list; refusing to write")
		os.Exit(1)
	}
	released := lic.Released
	if len(released) > 10 {
		released = released[:10]
	}
	var b strings.Builder
	fmt.Fprintf(&b, `# Official SPDX license and exception identifiers — GENERATED, do not hand-edit.
#
# Source: https://github.com/spdx/license-list-data (json/licenses.json and
# json/exceptions.json), license list version %s (released %s).
#
# Columns (tab-separated):
#   L <id> <osi> <deprecated>   a license; osi = isOsiApproved, 1 or 0
#   X <id> <deprecated>         a license exception
#
# Deprecated identifiers are INCLUDED: registries still emit them
# (GPL-2.0, LGPL-3.0, ...), and they are valid SPDX.
#
# Refresh (then check the diff of the osi column in review):
#   go run ./scripts/gen_spdx_data > internal/spdx/spdx_data.tsv
`, lic.Version, released)
	sort.Slice(lic.Licenses, func(i, j int) bool { return lic.Licenses[i].ID < lic.Licenses[j].ID })
	for _, l := range lic.Licenses {
		fmt.Fprintf(&b, "L\t%s\t%d\t%d\n", l.ID, bit(l.OSI), bit(l.Deprecated))
	}
	sort.Slice(exc.Exceptions, func(i, j int) bool { return exc.Exceptions[i].ID < exc.Exceptions[j].ID })
	for _, x := range exc.Exceptions {
		fmt.Fprintf(&b, "X\t%s\t%d\n", x.ID, bit(x.Deprecated))
	}
	if _, err := os.Stdout.WriteString(b.String()); err != nil {
		fmt.Fprintln(os.Stderr, "write:", err)
		os.Exit(1)
	}
}
