// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scantarget

import "testing"

func TestIsSelfDependency(t *testing.T) {
	selfSet := map[string]bool{
		"apache-airflow":                  true,
		"apache-airflow-providers-google": true,
		"airflow":                         true,
	}
	cases := []struct {
		name string
		want bool
	}{
		{"apache-airflow", true},
		{"Apache-Airflow", true}, // case-insensitive
		{"apache_airflow", true}, // '_'→'-' folded on the dep side
		{"apache-airflow-providers-google", true},
		{"apache-airflow-providers-amazon", false}, // exact only — NO prefix rule
		{"flask", false},
		{"airflow-lookalike", false},
	}
	for _, c := range cases {
		if got := IsSelfDependency(c.name, selfSet); got != c.want {
			t.Errorf("isSelfDependency(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}
