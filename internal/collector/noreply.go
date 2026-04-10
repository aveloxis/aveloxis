package collector

import (
	"regexp"
	"strconv"
	"strings"
)

// noreplyPattern matches GitHub noreply email addresses:
//   12345+username@users.noreply.github.com  -> (username, 12345)
//   username@users.noreply.github.com        -> (username, 0)
var noreplyPattern = regexp.MustCompile(
	`^(?:(\d+)\+)?([a-zA-Z0-9][a-zA-Z0-9._-]*)@users\.noreply\.github\.com$`,
)

// NoreplyInfo holds the parsed result of a GitHub noreply email.
type NoreplyInfo struct {
	Login    string
	UserID   int64 // 0 if not present in the email
	HasID    bool  // true if the numeric prefix was present
}

// ParseNoreplyEmail extracts GitHub login and optional user ID from a
// noreply email address. Returns nil if the email is not a noreply format.
//
// GitHub noreply emails come in two formats:
//   12345+username@users.noreply.github.com  (includes numeric user ID)
//   username@users.noreply.github.com        (login only)
//
// The numeric prefix is the gh_user_id, which is the stable identifier.
// The login can change (users can rename), but gh_user_id is permanent.
func ParseNoreplyEmail(email string) *NoreplyInfo {
	email = strings.TrimSpace(email)
	m := noreplyPattern.FindStringSubmatch(email)
	if m == nil {
		return nil
	}

	info := &NoreplyInfo{
		Login: m[2],
	}
	if m[1] != "" {
		id, err := strconv.ParseInt(m[1], 10, 64)
		if err == nil {
			info.UserID = id
			info.HasID = true
		}
	}
	return info
}

// IsNoreplyEmail returns true if the email is a GitHub noreply address.
func IsNoreplyEmail(email string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(email)), "@users.noreply.github.com")
}

// IsBotEmail returns true if the email looks like an automated/bot email
// that shouldn't be resolved to a human contributor.
func IsBotEmail(email string) bool {
	lower := strings.ToLower(email)
	return strings.Contains(lower, "[bot]") ||
		strings.Contains(lower, "noreply") && !strings.Contains(lower, "users.noreply.github.com") ||
		strings.HasSuffix(lower, "@github.com") && !strings.Contains(lower, "users.noreply")
}
