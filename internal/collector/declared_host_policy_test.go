// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/manifest"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// A host a collected repository's build file DECLARES gets none of the
// operator's trust (review round 1 of the vulnerability program's first
// part, security). The operator trusts hosts by name or by address range so
// that the forge instances and private mirrors they configured can be
// reached. A pom is written by whoever owns the repository: were a declared
// host dialed under that trust, `<url>https://the-trusted-host:8500/v1</url>`
// would be a way to make the server call an internal service.

// countingListener accepts and closes connections on loopback, counting them.
func countingListener(t *testing.T) (port string, accepted *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	accepted = &atomic.Int64{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			c.Close()
		}
	}()
	_, port, _ = net.SplitHostPort(ln.Addr().String())
	return port, accepted
}

// trustingPolicy installs a process rule that trusts one name and the
// loopback range, the name resolving to loopback.
func trustingPolicy(t *testing.T, name string) {
	t.Helper()
	p, err := platform.NewOutboundPolicy([]string{name, "127.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	old := currentOutboundPolicy()
	SetOutboundPolicy(p.WithResolver(func(context.Context, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	}))
	t.Cleanup(func() { SetOutboundPolicy(old) })
}

func TestADeclaredHostGetsNoneOfTheOperatorsTrust(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-no-trust")
	port, accepted := countingListener(t)
	trustingPolicy(t, "mirror.corp.example")
	withMavenRepositories(t)
	oldFetch := declaredRepositoryFetch.Load()
	SetDeclaredRepositoryFetch(true)
	t.Cleanup(func() { SetDeclaredRepositoryFetch(oldFetch) })

	for _, base := range []string{
		"https://mirror.corp.example:" + port + "/m2", // trusted by name
		"https://127.0.0.1:" + port + "/m2",           // trusted by range
		"https://other.example:" + port + "/m2",       // resolves into the trusted range
	} {
		_, outcome := src.RemotePom("org.remote", "parent", "5.0", []string{base})
		if outcome != manifest.RemoteRefused {
			t.Errorf("declared %s: outcome %v, want refused", base, outcome)
		}
	}
	if n := accepted.Load(); n != 0 {
		t.Fatalf("a declared host was connected to %d times under the operator's trust", n)
	}

	// The same address CONFIGURED by the operator is theirs to trust: it is
	// dialed (and fails here, the listener speaks no TLS).
	resetRegistryBreakers()
	withMavenRepositories(t, "https://mirror.corp.example:"+port+"/m2")
	src.RemotePom("org.remote", "parent", "6.0", nil)
	if accepted.Load() == 0 {
		t.Error("a configured repository at a trusted name was not dialed: the control did not run")
	}
	// And a connection made for the configured repository is never reused
	// for a declared address at the same host and port.
	before := accepted.Load()
	withMavenRepositories(t)
	if _, outcome := src.RemotePom("org.remote", "parent", "7.0", []string{"https://mirror.corp.example:" + port + "/elsewhere"}); outcome != manifest.RemoteRefused {
		t.Errorf("declared, after the configured one was dialed: outcome %v, want refused", outcome)
	}
	if accepted.Load() != before {
		t.Error("a declared address was dialed after a configured one at the same host")
	}
}

// A redirect from a declared host is judged the same way, hop by hop.
func TestARedirectFromADeclaredHostGetsNoTrust(t *testing.T) {
	trustingPolicy(t, "mirror.corp.example")
	hop := func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://mirror.corp.example/secret", nil)
		if err != nil {
			t.Fatal(err)
		}
		return registryCheckRedirect(req, nil)
	}
	if err := hop(withDeclaredRepositoryHost(context.Background())); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Errorf("a hop from a declared host to a trusted internal name: %v, want refused as a private address", err)
	}
	if err := hop(context.Background()); err != nil {
		t.Errorf("the same hop from a configured repository: %v, want followed", err)
	}
	// A declared request is https on every hop.
	if !registryHTTPSOnly(withDeclaredRepositoryHost(context.Background())) {
		t.Error("a declared request must be https-only")
	}
}

// The daily check looks at a host under the rule a fetch from it is made
// under: a configured repository's with the operator's trust, any other
// host's with none.
func TestTheHostProbeUsesTheFetchRule(t *testing.T) {
	trustingPolicy(t, "mirror.corp.example")
	withMavenRepositories(t, "https://Mirror.Corp.Example:8443/m2")
	ctx := context.Background()
	if err := repositoryHostProbePolicy("mirror.corp.example:8443").RefuseHost(ctx, "mirror.corp.example:8443"); err != nil {
		t.Errorf("a configured repository is refused by its own probe: %v", err)
	}
	for _, host := range []string{"mirror.corp.example", "mirror.corp.example:9000", "declared.example"} {
		if err := repositoryHostProbePolicy(host).RefuseHost(ctx, host); !errors.Is(err, platform.ErrPrivateAddress) {
			t.Errorf("%s is not a configured repository, and its probe was not refused (%v)", host, err)
		}
	}
}

// The seam's default is the strict rule: nothing the process trusts.
func TestTheDeclaredHostPolicyIsStrictByDefault(t *testing.T) {
	trustingPolicy(t, "mirror.corp.example")
	p := declaredHostPolicy()
	for _, host := range []string{"mirror.corp.example", "127.0.0.1", "repo.maven.apache.org", "anything.example"} {
		if err := p.RefuseHost(context.Background(), host); !errors.Is(err, platform.ErrPrivateAddress) {
			t.Errorf("the declared-host rule let %s through (%v); every name here resolves to loopback", host, err)
		}
	}
	if got := p.Allow(); len(got) != 0 {
		t.Errorf("the declared-host rule trusts %v", got)
	}
}
