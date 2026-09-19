// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package srctest

import "testing"

// TestAnglePlaceholderRegexShape is the tripwire's own contract, both
// arms: every shape that broke a pasted line in the corpus must fire,
// and every legitimate use of `<` in a shell block must not — a
// broader regex that flags real redirections would be "fixed" by
// rewording working commands.
func TestAnglePlaceholderRegexShape(t *testing.T) {
	fires := []string{
		"psql -h <host> -p 5434 -U aveloxis",
		"diff perf-<previous>.txt perf-$(date +%Y%m%d).txt",
		"aveloxis sbom <repo-id>       # generate SBOMs",
		"go install github.com/aveloxis/aveloxis/cmd/aveloxis@v<primary version>",
		"  SELECT * FROM aveloxis_data.<flagged_table>",
		"  WHERE <pk> NOT IN (SELECT <pk> FROM x)",
		"scancode -clpi --json <output-file> --quiet <path>",
		"cd <checkout> && go install ./cmd/aveloxis",
		"aveloxis add-key [flags] [<token>]",
	}
	for _, line := range fires {
		if AnglePlaceholder.FindString(line) == "" {
			t.Errorf("must flag the placeholder in %q", line)
		}
	}
	quiet := []string{
		"cmd <in.txt >out.txt",
		"sort <input.txt > sorted.txt",
		"cat <<EOF > aveloxis.json",
		"cat <<'EOF'",
		"diff <(aveloxis version) <(cat expected)",
		"some-command 2<&1",
		"echo $(<file)",
		"psql -c \"SELECT 1 <> 2\"",
		"go test ./... -run 'TestX' >/dev/null",
	}
	for _, line := range quiet {
		if m := AnglePlaceholder.FindString(line); m != "" {
			t.Errorf("must NOT flag %q in the legitimate shell line %q", m, line)
		}
	}
}
