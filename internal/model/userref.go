package model

// UserRef is a lightweight reference to a platform user, carried through from
// API responses so the collector can resolve it to a contributor UUID.
type UserRef struct {
	PlatformID int64
	Login      string
	Name       string
	Email      string
	AvatarURL  string
	URL        string
	NodeID     string
	Type       string // "User", "Bot", "Organization"
}

// IsZero reports whether u contains no identifying information.
func (u UserRef) IsZero() bool {
	return u.PlatformID == 0 && u.Login == ""
}
