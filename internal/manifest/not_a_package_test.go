// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

func readOne(t *testing.T, name, body string) []model.LibyearDep {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	d := NewWalk(dir, nil).Read(path, Options{DevBuildDeps: true})
	if d.Failure != nil {
		t.Fatalf("%s: %v", name, d.Failure)
	}
	return d.Deps
}

// The production log showed two things read as package names that are not
// packages (summary/69): a template expression sent to PyPI, and a project
// file's path sent to NuGet.

// A template placeholder is not a declaration. A project template
// (cookiecutter, copier) holds manifests whose "names" are expressions to be
// filled in later; they name no package, and are not dependencies whose name
// we failed to read either, so they leave no row and no note.
func TestATemplateExpressionIsNotADependency(t *testing.T) {
	for name, body := range map[string]string{
		"Pipfile":          "[packages]\nrequests = \"*\"\n\"{% if cookiecutter.use_ruff %}\" = \"*\"\n\"{{ cookiecutter.pkg }}\" = \"*\"\n",
		"requirements.txt": "requests==2.0\n{{cookiecutter.project_slug}}==1.0\n{% if cookiecutter.use_ruff %}\n",
		"package.json":     `{"dependencies": {"left-pad": "1.0.0", "{{ cookiecutter.pkg }}": "1.0.0"}}`,
	} {
		deps := readOne(t, name, body)
		if len(deps) != 1 {
			t.Errorf("%s: %d dependencies, want only the real one", name, len(deps))
		}
		for _, d := range deps {
			if isTemplateExpression(d.Name) {
				t.Errorf("%s: a template expression was read as the dependency %q", name, d.Name)
			}
		}
	}
	// Braces alone are not a template: a real name may not hold them, and
	// such a name is left for the name check to refuse and record.
	for _, n := range []string{"requests", "@scope/pkg", "a{b}", "100%", "{single}"} {
		if isTemplateExpression(n) {
			t.Errorf("%q is not a template expression", n)
		}
	}
}

// A project file's path in a PackageReference is a reference to another
// project in the tree, not a package on NuGet: it is inventoried as a
// dependency no registry holds (no lookup, no purl), like a path dependency
// in any other ecosystem.
func TestAProjectPathIsNotANuGetPackage(t *testing.T) {
	deps := readOne(t, "app.csproj", `<Project><ItemGroup>
<PackageReference Include="Newtonsoft.Json" Version="13.0.1" />
<PackageReference Include="..\Other\Other.csproj" Version="1.0" />
<PackageReference Include="../Lib/Lib.fsproj" />
<PackageReference Include="Local.vbproj" />
</ItemGroup></Project>`)
	if len(deps) != 4 {
		t.Fatalf("%d dependencies, want 4", len(deps))
	}
	for _, d := range deps {
		want := d.Name != "Newtonsoft.Json"
		if d.NonRegistry != want {
			t.Errorf("%q: non-registry %v, want %v", d.Name, d.NonRegistry, want)
		}
	}
}
