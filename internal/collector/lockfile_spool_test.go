// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// heldLockfiles reads a lockfile set's parsed lockfiles into memory, as the
// tests compare them: the child's are on disk (lockfile_spool.go).
func heldLockfiles(set lockfileSet) []parsedLockfile {
	var out []parsedLockfile
	if err := set.each(func(pl parsedLockfile) error {
		out = append(out, pl)
		return nil
	}); err != nil {
		panic(err)
	}
	return out
}
