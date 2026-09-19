// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package a is a fixture for TestCheckTestDirectoryUsesVariants: a package
// whose external test imports it, a dependent of it (package b), and a
// package that reaches it only through b (package c).
package a

// K is a type the external test passes between a and b.
type K struct{}

// New returns a K.
func New() *K { return &K{} }
