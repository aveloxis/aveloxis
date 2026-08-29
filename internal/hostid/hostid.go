// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package hostid reads the kernel boot id for the (pid, boot_id) locks
// the scancode worker (v0.21.0 — its orphan recovery compares it) and
// the mailing-list worker (v0.25.7 — informational; nothing compares it,
// the shutdown release is keyed on the claim's own lock stamp, and no
// migrate-time rule may be built on it: PIDs are namespaced and the
// boot id host-global under the container deployment) stamp. One
// reader (SR-17).
package hostid

import (
	"os"
	"strings"
)

// BootIDPath is the Linux kernel's per-boot UUID.
const BootIDPath = "/proc/sys/kernel/random/boot_id"

// BootID returns the kernel boot UUID on Linux and "" elsewhere (macOS
// dev machines): callers treat "" as "unknown, cannot make a same-host
// decision" and fall through to their other signal.
func BootID() string {
	data, err := os.ReadFile(BootIDPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}
