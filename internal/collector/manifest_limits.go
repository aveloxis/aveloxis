// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "sync/atomic"

// The limits on the manifest-reading child, as loadConfig installed them
// (collection.manifest_read_cpu_seconds, collection.manifest_read_memory_mb).
// Before it does, and in a program that loads no configuration, they are
// the defaults (ManifestLimits.effective).
var installedManifestLimits atomic.Pointer[ManifestLimits]

// SetManifestLimits installs the limits.
func SetManifestLimits(l ManifestLimits) { installedManifestLimits.Store(&l) }

func manifestLimits() ManifestLimits {
	if l := installedManifestLimits.Load(); l != nil {
		return *l
	}
	return ManifestLimits{}
}
