// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package manifest

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// Review round 8 (boundary, handed over): a composer.json whose path
// repository holds a composer.json that is THERE and could not be read does
// not know which of its requirements that directory supplies. It was read
// as if the directory named no package, and a requirement the clone itself
// supplies was inventoried as Packagist's package of that name (SR-5: a
// read that failed is not "no such file"). The dependent composer.json is
// now a manifest that could not be read, and the error names the file.
func TestAComposerPathRepositoryThatCannotBeReadIsNotAnEmptyOne(t *testing.T) {
	files := map[string]string{
		"app/composer.json":          `{"require":{"acme/local":"*","monolog/monolog":"^3"},"repositories":[{"type":"path","url":"../packages/*"}]}`,
		"packages/one/composer.json": `{"name":"acme/local","description":"` + strings.Repeat("x", 4096) + `"}`,
		"packages/two/README":        "no composer.json here: a directory that names no package\n",
	}
	root := writeTree(t, files)
	app := filepath.Join(root, "app", "composer.json")

	// The control: everything is read, and acme/local is the clone's own.
	deps, err := parseComposerJSONVersionsIn(root, app)
	if err != nil {
		t.Fatal(err)
	}
	own := map[string]bool{}
	for _, d := range deps {
		own[d.Name] = d.NonRegistry
	}
	if !own["acme/local"] || own["monolog/monolog"] || len(deps) != 2 {
		t.Fatalf("read whole: %v; want acme/local the clone's own and monolog Packagist's", own)
	}

	// The process's read bound refuses the path repository's file and not
	// the manifest itself.
	restore := SetReadBound(func(path string, size int64) int64 { return 1024 })
	defer restore()
	deps, err = parseComposerJSONVersionsIn(root, app)
	if err == nil {
		t.Fatalf("a path repository's composer.json was refused by the read bound, and the manifest was read as %v", deps)
	}
	if !errors.Is(err, errComposerPartUnread) || !strings.Contains(err.Error(), "packages/one/composer.json") {
		t.Errorf("the error is %q; want errComposerPartUnread naming packages/one/composer.json", err)
	}
	var tooLarge *ManifestTooLargeError
	if !errors.As(err, &tooLarge) {
		t.Errorf("the error does not carry why the file was not read: %v", err)
	}
	if len(deps) != 0 {
		t.Errorf("%d rows were taken from a manifest that could not be read whole", len(deps))
	}
}

// One over MaxBuildFileBytes is there and is not read, the same.
func TestAComposerPathRepositoryFileOverTheBuildFileBoundIsNotAnEmptyOne(t *testing.T) {
	root := writeTree(t, map[string]string{
		"app/composer.json":          `{"require":{"acme/local":"*"},"repositories":[{"type":"path","url":"../packages/one"}]}`,
		"packages/one/composer.json": `{"name":"acme/local","description":"` + strings.Repeat("x", int(MaxBuildFileBytes)) + `"}`,
	})
	if deps, err := parseComposerJSONVersionsIn(root, filepath.Join(root, "app", "composer.json")); !errors.Is(err, errComposerPartUnread) {
		t.Errorf("read as %v, %v; want errComposerPartUnread", deps, err)
	}
	// A directory with no composer.json, and a pattern that matches nothing,
	// name no package and are no failure.
	root = writeTree(t, map[string]string{
		"app/composer.json": `{"require":{"acme/local":"*"},"repositories":[{"type":"path","url":"../packages/*"},{"type":"path","url":"../nowhere"}]}`,
		"packages/empty/x":  "",
	})
	if deps, err := parseComposerJSONVersionsIn(root, filepath.Join(root, "app", "composer.json")); err != nil || len(deps) != 1 || deps[0].NonRegistry {
		t.Errorf("read as %v, %v; want one Packagist requirement and no error", deps, err)
	}
}
