package platform

import (
	"bytes"
	"strings"
)

// rateLimitBodyMarkers are case-insensitive substrings that, when present in
// a 403 response body, indicate the response is a rate-limit signal even
// when the headers fail to convey it. GitHub publishes several distinct
// wordings depending on whether the limit is the core hourly bucket, the
// secondary "abuse" detector, or the unauthenticated per-IP bucket.
//
// Used as a fallback after the header-based detection (Retry-After /
// X-RateLimit-Remaining=0) returns no signal — never as the primary path.
// The header path remains authoritative because it carries the precise
// reset timing that Retry-After plumbing relies on.
var rateLimitBodyMarkers = []string{
	"rate limit exceeded",
	"secondary rate limit",
	"abuse detection",
}

// anonymousRateLimitMarkers are the narrower phrases that only appear on
// rate-limit responses for unauthenticated requests. If we see one of these
// from inside an HTTPClient that always sets an Authorization header, we
// have a key-leak bug — some code path is constructing an unauthenticated
// request against this client. The httpclient logs this at ERROR level so
// the regression is visible in standard log scraping.
// "authenticated requests get a higher" is the only safe marker — GitHub
// adds that hint exclusively when telling the caller they're anonymous.
// The "API rate limit exceeded for <X>" prefix is shared with the
// authenticated-user variant ("for user ID 12345") and would over-fire.
var anonymousRateLimitMarkers = []string{
	"authenticated requests get a higher",
}

// isRateLimitBody reports whether body contains any of the rate-limit
// substrings. Case-insensitive.
func isRateLimitBody(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	lowered := bytes.ToLower(body)
	for _, m := range rateLimitBodyMarkers {
		if bytes.Contains(lowered, []byte(m)) {
			return true
		}
	}
	return false
}

// isAnonymousRateLimitBody reports whether body matches the unauthenticated
// per-IP rate-limit shape. Strictly narrower than isRateLimitBody — must
// not fire on user-token rate limits.
func isAnonymousRateLimitBody(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	lowered := strings.ToLower(string(body))
	for _, m := range anonymousRateLimitMarkers {
		if strings.Contains(lowered, m) {
			return true
		}
	}
	return false
}
