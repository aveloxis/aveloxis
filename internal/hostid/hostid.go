// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package hostid identifies the machine a process runs on for the
// (pid, boot_id) crash-recovery locks the scancode and mailing-list
// workers stamp (v0.21.0, v0.25.7) and the migrate-time liveness
// decisions that read them (v0.28.18). One reader (SR-17): a lock whose
// boot_id equals this host's is adjudicated by PID liveness; any other
// boot_id is another host (or unknown) and needs a database-side signal.
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
