// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"log/slog"
	"sync"
	"time"
)

// jobSlots records each running job's start and current phase (worklist
// item 77, observation only): a 10-hour facade held a slot with nothing
// logged between "job started" and its end, and "job interrupted" did not
// say how long the job had run. The zero value is ready to use.
type jobSlots struct {
	mu sync.Mutex
	m  map[int64]*jobSlot // repo_id → slot
}

type jobSlot struct {
	start time.Time
	phase string
}

func (j *jobSlots) beginAt(repoID int64, start time.Time) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.m == nil {
		j.m = make(map[int64]*jobSlot)
	}
	j.m[repoID] = &jobSlot{start: start, phase: "prelim"}
}

func (j *jobSlots) setPhase(repoID int64, phase string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if s, ok := j.m[repoID]; ok {
		s.phase = phase
	}
}

func (j *jobSlots) end(repoID int64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	delete(j.m, repoID)
}

// elapsed is how long the job has run (0 when it is not registered).
func (j *jobSlots) elapsed(repoID int64) time.Duration {
	j.mu.Lock()
	defer j.mu.Unlock()
	if s, ok := j.m[repoID]; ok {
		return time.Since(s.start)
	}
	return 0
}

// log writes one INFO line: the active slots, how many are in each phase,
// and the oldest one — the head-of-line question (which job has held its
// slot longest, and doing what). Nothing when no job runs.
func (j *jobSlots) log(logger *slog.Logger) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if len(j.m) == 0 {
		return
	}
	perPhase := map[string]int{}
	var oldestID int64
	var oldest *jobSlot
	for id, s := range j.m {
		perPhase[s.phase]++
		if oldest == nil || s.start.Before(oldest.start) {
			oldestID, oldest = id, s
		}
	}
	phases := make([]any, 0, 2*len(perPhase))
	for p, n := range perPhase {
		phases = append(phases, slog.Int(p, n))
	}
	logger.Info("collection slots",
		"active", len(j.m),
		"oldest_repo_id", oldestID, "oldest_phase", oldest.phase,
		"oldest_age", time.Since(oldest.start).Round(time.Second),
		slog.Group("per_phase", phases...))
}
