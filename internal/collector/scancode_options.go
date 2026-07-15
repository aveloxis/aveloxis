// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"github.com/aveloxis/aveloxis/internal/config"
)

// ScancodeOptionsFromConfig maps the operator's aveloxis.json
// collection block onto ScancodeWorkerOptions through the config
// accessors (defaults applied at the accessor layer, per the
// config-knob end-to-end lesson).
//
// v0.27.6: this is THE single mapping, shared by both spawn sites —
// the scheduler (aveloxis serve) and the dedicated
// `aveloxis scancode-worker` command — so the two can never drift on
// which knob feeds which field.
func ScancodeOptionsFromConfig(c *config.CollectionConfig) ScancodeWorkerOptions {
	return ScancodeWorkerOptions{
		Workers:           c.ScancodeWorkersOrDefault(),
		StartInterval:     c.ScancodeStartInterval(),
		Cadence:           c.ScancodeCadence(),
		CloneDir:          c.ScancodeCloneDirOrDefault(),
		ShutdownGrace:     c.ScancodeShutdownGrace(),
		RunTimeoutBase:    c.ScancodeRunTimeout(),
		RunTimeoutCap:     c.ScancodeRunTimeoutCap(),
		MaxInMemory:       c.ScancodeMaxInMemoryOrDefault(),
		TimeoutCapStrikes: c.ScancodeTimeoutCapStrikesOrDefault(),
		IgnoreGlobs:       c.ScancodeIgnoreGlobsOrDefault(),
	}
}
