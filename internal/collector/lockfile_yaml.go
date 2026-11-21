// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v3"
)

// How a YAML lockfile's mappings are read (0.29.213).
//
// yaml.v3 decodes a mapping into a Go map or struct only after comparing
// every key of it with every other (its duplicate-key check), so a mapping
// of n keys costs n*n/2 comparisons: a pnpm-lock.yaml of 177,000 packages,
// fifty megabytes and an honest file, took 132 seconds of CPU to decode, and
// two megabytes of short keys take as long. Decoding into a yaml.Node makes
// no such check, so the document is parsed to nodes and its mappings are
// walked here, with the same check made through a map.
//
// What is kept of the decoder's reading, for the nodes a lockfile's reader
// visits: a key that occurs twice in one mapping (the same kind and text) is
// an error; a null where a mapping or a string is expected is an empty one;
// anything else of the wrong kind is an error; a scalar is read by the
// decoder itself (yamlString), so a number is its text as written. What a
// reader does not visit is not judged, as before.
//
// What is NOT kept: an alias (*name) or a merge key (<<) in what a reader
// visits is an error, where the decoder followed it. No package manager
// writes either in a lockfile, and following them is how a small YAML file
// becomes a large one.
var (
	errYAMLLockAlias = errors.New("the lockfile uses a YAML alias or merge key, which no package manager writes in a lockfile; it is refused on purpose, and its previous rows are kept")
	errYAMLLockKind  = errors.New("the lockfile holds a value of another kind where a mapping or a string belongs")
)

// yamlPair is one key of a mapping and its value's node.
type yamlPair struct {
	key string
	val *yaml.Node
}

// yamlLockRoot parses a document and returns its root's pairs: none for an
// empty document.
func yamlLockRoot(data []byte) ([]yamlPair, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, err
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) == 0 {
		return nil, nil
	}
	return yamlPairs(doc.Content[0])
}

// yamlIsNull: a node the decoder reads as null. A scalar tagged !!null is
// one only when its text is a null's ("", ~, null): `!!null x` is not, and
// the decoder refuses it, as yamlString then does (round 7: it was read as
// a null).
func yamlIsNull(n *yaml.Node) bool {
	if n == nil || n.Kind == 0 {
		return true
	}
	if n.Kind != yaml.ScalarNode || n.ShortTag() != "!!null" {
		return false
	}
	switch n.Value {
	case "", "~", "null", "Null", "NULL":
		return true
	}
	return false
}

// yamlPairs is a mapping's keys and values, in the document's order. A nil
// or null node has none.
func yamlPairs(n *yaml.Node) ([]yamlPair, error) {
	if yamlIsNull(n) {
		return nil, nil
	}
	switch n.Kind {
	case yaml.AliasNode:
		return nil, errYAMLLockAlias
	case yaml.MappingNode:
	default:
		return nil, errYAMLLockKind
	}
	// A key written twice (the same kind and text) is an error, as the
	// decoder's own check has it. Two keys written differently that DECODE
	// to one string (`a` and `!!binary YQ==`) are one key of the map the
	// decoder filled, holding the later value: so here (round 7: they were
	// two rows). Its place is the first one's.
	seen := make(map[string]int, len(n.Content)/2)
	at := make(map[string]int, len(n.Content)/2)
	out := make([]yamlPair, 0, len(n.Content)/2)
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Kind == yaml.AliasNode || (k.Kind == yaml.ScalarNode && k.Value == "<<" && (k.Tag == "" || k.Tag == "!" || k.ShortTag() == "!!merge")) {
			return nil, errYAMLLockAlias
		}
		id := string(rune('0'+k.Kind)) + k.Value
		if line, dup := seen[id]; dup {
			return nil, fmt.Errorf("line %d: mapping key %q already defined at line %d", k.Line, k.Value, line)
		}
		seen[id] = k.Line
		if yamlIsNull(k) {
			continue // a null is no key of a mapping of strings: the decoder left the pair out
		}
		key, err := yamlString(k)
		if err != nil {
			return nil, err
		}
		if first, again := at[key]; again {
			out[first].val = v
			continue
		}
		at[key] = len(out)
		out = append(out, yamlPair{key, v})
	}
	return out, nil
}

// yamlString is a scalar as the decoder gives it to a string: its text as
// written, and nothing for a null.
func yamlString(n *yaml.Node) (string, error) {
	if yamlIsNull(n) {
		return "", nil
	}
	switch n.Kind {
	case yaml.AliasNode:
		return "", errYAMLLockAlias
	case yaml.ScalarNode:
	default:
		return "", errYAMLLockKind
	}
	var s string
	if err := n.Decode(&s); err != nil {
		return "", err
	}
	return s, nil
}

// yamlField is the value of a mapping's key, or nil.
func yamlField(pairs []yamlPair, name string) *yaml.Node {
	for _, p := range pairs {
		if p.key == name {
			return p.val
		}
	}
	return nil
}

// yamlStringMap reads a mapping of strings.
func yamlStringMap(n *yaml.Node) ([]yamlPair, []string, error) {
	pairs, err := yamlPairs(n)
	if err != nil {
		return nil, nil, err
	}
	values := make([]string, len(pairs))
	for i, p := range pairs {
		if values[i], err = yamlString(p.val); err != nil {
			return nil, nil, err
		}
	}
	return pairs, values, nil
}

// yamlItems is a sequence's items. A nil or null node has none.
func yamlItems(n *yaml.Node) ([]*yaml.Node, error) {
	if yamlIsNull(n) {
		return nil, nil
	}
	switch n.Kind {
	case yaml.AliasNode:
		return nil, errYAMLLockAlias
	case yaml.SequenceNode:
		return n.Content, nil
	}
	return nil, errYAMLLockKind
}
