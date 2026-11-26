// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostguard

import (
	"reflect"
	"testing"
)

func TestMayUse(t *testing.T) {
	list := NewSnapshot([]string{"Bad.Example", " worse.example. ", "nexus.corp.example:8443", ""})
	for host, want := range map[string]bool{
		"good.example":            true,
		"bad.example":             false,
		"BAD.EXAMPLE":             false, // one spelling
		"bad.example.":            false,
		"worse.example":           false,
		"nexus.corp.example:8443": false,
		"nexus.corp.example":      true, // another port is another host
		"sub.bad.example":         true, // a name is exact
		"":                        false,
	} {
		if got := MayUse(list, host); got != want {
			t.Errorf("MayUse(%q) = %v, want %v", host, got, want)
		}
	}
	// No list distrusts nothing; an empty host is still not usable.
	if !MayUse(nil, "anything.example") || MayUse(nil, "") {
		t.Error("a nil list distrusts nothing, and an empty host is never usable")
	}
	if len(list) != 3 {
		t.Errorf("the snapshot holds %d hosts, want 3 (the empty one is not a host)", len(list))
	}
}

func TestAllowedIsTheEffectiveList(t *testing.T) {
	host := func(base string) string { return base[len("https://") : len(base)-len("/m2")] }
	bases := []string{"https://central.example/m2", "https://bad.example/m2", "https://jenkins.example/m2"}
	got := Allowed(NewSnapshot([]string{"bad.example"}), bases, host)
	if want := []string{"https://central.example/m2", "https://jenkins.example/m2"}; !reflect.DeepEqual(got, want) {
		t.Errorf("effective list = %v, want %v (in order, without the distrusted one)", got, want)
	}
	if got := Allowed(nil, bases, host); !reflect.DeepEqual(got, bases) {
		t.Errorf("with no list: %v", got)
	}
}
