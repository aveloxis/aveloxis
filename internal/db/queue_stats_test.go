package db

import (
	"testing"
)

// TestQueueJobHasCommitsField verifies the QueueJob struct tracks commit counts.
func TestQueueJobHasCommitsField(t *testing.T) {
	j := QueueJob{
		LastCommits: 42,
	}
	if j.LastCommits != 42 {
		t.Errorf("QueueJob.LastCommits = %d, want 42", j.LastCommits)
	}
}
