package collector

// parallelSlotsForWorkers returns how many extra ParallelSlots a
// large-repo parallel collection should claim, given the scheduler's
// total worker count.
//
// The claim serves two purposes simultaneously:
//
//  1. Represent actual in-flight work: collectParallel forks 3
//     goroutines (issues, PRs, events), so the floor is 3.
//
//  2. Signal "reserve bandwidth" to the scheduler: fillWorkerSlots
//     stops claiming new jobs when len(sem)+extraSlots >= workers.
//     A fixed claim of 3 on an 80-worker pool throttles only at 77
//     active — effectively never — so the large repo's three
//     goroutines end up competing with 79 other jobs for DB
//     connections and API headroom. Scaling the claim to ~10% of
//     the pool (8 on workers=80) makes the throttle kick in at 72
//     active, actually reserving capacity for the big repo.
//
// The 30-worker break point is where legacy and scaled claims agree:
// 30/10 == 3. Below that, returning workers/10 would drop below the
// goroutine floor.
func parallelSlotsForWorkers(workers int) int {
	const legacyClaim = 3
	if workers <= 30 {
		return legacyClaim
	}
	scaled := workers / 10
	if scaled < legacyClaim {
		return legacyClaim
	}
	return scaled
}
