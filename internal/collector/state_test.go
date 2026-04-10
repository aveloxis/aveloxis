package collector

import "testing"

func TestCollectionStatusValues(t *testing.T) {
	tests := []struct {
		status CollectionStatus
		want   string
	}{
		{StatusPending, "Pending"},
		{StatusInitializing, "Initializing"},
		{StatusCollecting, "Collecting"},
		{StatusSuccess, "Success"},
		{StatusError, "Error"},
	}
	for _, tc := range tests {
		if got := string(tc.status); got != tc.want {
			t.Errorf("CollectionStatus = %q, want %q", got, tc.want)
		}
	}
}

func TestPhaseValues(t *testing.T) {
	tests := []struct {
		phase Phase
		want  string
	}{
		{PhasePrelim, "prelim"},
		{PhasePrimary, "primary"},
		{PhaseSecondary, "secondary"},
		{PhaseFacade, "facade"},
	}
	for _, tc := range tests {
		if got := string(tc.phase); got != tc.want {
			t.Errorf("Phase = %q, want %q", got, tc.want)
		}
	}
}
