// Package collector orchestrates data collection across platforms.
package collector

// CollectionStatus tracks the state of a repo's collection process.
type CollectionStatus string

const (
	StatusPending      CollectionStatus = "Pending"
	StatusInitializing CollectionStatus = "Initializing"
	StatusCollecting   CollectionStatus = "Collecting"
	StatusSuccess      CollectionStatus = "Success"
	StatusError        CollectionStatus = "Error"
)

// Phase identifies a collection phase.
type Phase string

const (
	PhasePrelim    Phase = "prelim"
	PhasePrimary   Phase = "primary"
	PhaseSecondary Phase = "secondary"
	PhaseFacade    Phase = "facade"
)
