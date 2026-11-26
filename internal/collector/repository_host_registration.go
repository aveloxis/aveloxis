// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostwatch"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// rdapBodyLimit bounds one registration-data document read into memory.
// IANA's bootstrap file is tens of kilobytes and a domain object a few; four
// megabytes is far above either and far below anything that matters to the
// process.
const rdapBodyLimit int64 = 4 << 20

// rdapBootstrapURL is where the bootstrap file is fetched from: a var so a
// test can point it elsewhere.
var rdapBootstrapURL = hostwatch.RDAPBootstrapURL

var errRegistrationDataNotHTTPS = errors.New("registration data is fetched over https only")

// fetchRegistrationData is the registration-date check's one request path
// (0.29.190; summary/70, 4.5, D14). The servers are neither fixed registries
// nor operator-configured (IANA's bootstrap file names them), so nothing
// here is trusted by name: the request is https, every hop is judged by the
// outbound-address rule and stays on https, and the body is bounded.
func fetchRegistrationData(ctx context.Context, rawURL string) ([]byte, error) {
	if u, err := url.Parse(rawURL); err != nil || u.Scheme != "https" || u.User != nil {
		return nil, fmt.Errorf("%w: %s", errRegistrationDataNotHTTPS, platform.RedactURLUserinfo(rawURL))
	}
	ctx = withRegistryHTTPSOnly(withRegistryBodyLimit(ctx, rdapBodyLimit))
	return fetchRegistryJSON(ctx, rawURL, "Accept: application/rdap+json, application/json")
}

// RepositoryHostRegistration makes one pass's registration-date check: the
// bootstrap file and each domain are asked once per value.
func RepositoryHostRegistration() func(ctx context.Context, host string, firstFetched time.Time) hostwatch.Observation {
	r := &hostwatch.Registration{
		Get:          fetchRegistrationData,
		NotFound:     func(err error) bool { return errors.Is(err, errRegistryNotFound) },
		BootstrapURL: rdapBootstrapURL,
	}
	return r.Check
}
