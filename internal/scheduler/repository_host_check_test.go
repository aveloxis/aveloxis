// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/hostwatch"
)

type fakeHostCheck struct {
	mu         sync.Mutex
	answered   []string
	distrusted []string
	listErr    error
	since      time.Duration
	ever       bool
	sinceErr   error
	markErr    error
	recorded   map[string][]db.HostObservation
	marks      []int
	onMark     func()
}

func (f *fakeHostCheck) RepositoryHostFirstFetches(context.Context) (map[string]time.Time, error) {
	out := map[string]time.Time{}
	for _, h := range f.answered {
		out[strings.ToLower(h)] = fakeFirstFetch
	}
	return out, nil
}

var fakeFirstFetch = time.Date(2025, 1, 2, 0, 0, 0, 0, time.UTC)

func (f *fakeHostCheck) GetDistrustedRepositoryHosts(context.Context) ([]string, error) {
	return f.distrusted, f.listErr
}
func (f *fakeHostCheck) RecordRepositoryHostObservations(_ context.Context, host string, obs []db.HostObservation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recorded == nil {
		f.recorded = map[string][]db.HostObservation{}
	}
	f.recorded[host] = obs
	return nil
}
func (f *fakeHostCheck) SinceRepositoryHostCheck(context.Context) (time.Duration, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.since, f.ever, f.sinceErr
}
func (f *fakeHostCheck) MarkRepositoryHostCheckDone(_ context.Context, hosts int) error {
	f.mu.Lock()
	f.marks = append(f.marks, hosts)
	f.since, f.ever = 0, true
	on := f.onMark
	f.mu.Unlock()
	if on != nil {
		on()
	}
	return f.markErr
}

func probeAll(state hostwatch.State, probed *[]string) func(context.Context, string) []hostwatch.Observation {
	return func(_ context.Context, host string) []hostwatch.Observation {
		*probed = append(*probed, host)
		return []hostwatch.Observation{{Kind: hostwatch.KindCertificate, State: state, Detail: "d"}}
	}
}

// One pass: the configured hosts and the ones that answered, each once,
// never a distrusted one; what was seen is stored and the pass marked.
func TestRepositoryHostCheckPass(t *testing.T) {
	f := &fakeHostCheck{answered: []string{"declared.example", "Central.Example", "gone.example"}, distrusted: []string{"gone.example", "bad-configured.example"}}
	var probed []string
	log := &recLog{}
	c := &repositoryHostCheck{store: f, probe: probeAll(hostwatch.Signal, &probed), logger: log, every: time.Hour, on: true,
		configured: []string{"central.example", "bad-configured.example"}, declared: true, pause: time.Millisecond}
	if !c.pass(context.Background()) {
		t.Fatal("the pass did not finish")
	}
	if got := strings.Join(probed, ","); got != "central.example,declared.example" {
		t.Errorf("probed %q; want the configured host and the declared one, each once, and neither distrusted host", got)
	}
	if o := f.recorded["declared.example"]; len(o) != 1 || o[0].Kind != "certificate" || o[0].State != "signal" || o[0].Detail != "d" {
		t.Errorf("recorded for the declared host: %+v", o)
	}
	if len(f.marks) != 1 || f.marks[0] != 2 || !log.has("pass complete") {
		t.Errorf("marks %v, log %v", f.marks, log.lines)
	}

	// With declared fetching off, only the configured hosts are contacted.
	probed = nil
	c.declared = false
	c.pass(context.Background())
	if got := strings.Join(probed, ","); got != "central.example" {
		t.Errorf("with declared_repository_fetch off, probed %q; want the configured host only", got)
	}
}

// If the distrusted list cannot be read, no host is contacted (SR-5), and
// the pass is not marked done.
func TestRepositoryHostCheckContactsNothingWithoutTheDistrustedList(t *testing.T) {
	f := &fakeHostCheck{answered: []string{"a.example"}, listErr: errors.New("connection reset")}
	var probed []string
	log := &recLog{}
	c := &repositoryHostCheck{store: f, probe: probeAll(hostwatch.Clear, &probed), logger: log, every: time.Hour, on: true, configured: []string{"central.example"}, declared: true}
	if c.pass(context.Background()) || len(probed) != 0 || len(f.marks) != 0 || !log.has("could not be listed") {
		t.Errorf("probed %v, marks %v, log %v", probed, f.marks, log.lines)
	}
}

// Shutdown during a pass is not a failure and marks nothing.
func TestRepositoryHostCheckStopsOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := &fakeHostCheck{answered: []string{"a.example", "b.example"}}
	log := &recLog{}
	c := &repositoryHostCheck{store: f, logger: log, every: time.Hour, on: true, declared: true, pause: time.Millisecond,
		probe: func(context.Context, string) []hostwatch.Observation {
			cancel()
			return []hostwatch.Observation{{Kind: hostwatch.KindCertificate, State: hostwatch.Unknown, Detail: "context canceled"}}
		}}
	if c.pass(ctx) || len(f.recorded) != 0 || len(f.marks) != 0 || len(log.lines) != 0 {
		t.Errorf("after shutdown: recorded %v, marks %v, log %v; want nothing stored (a probe cut short saw nothing) and no line", f.recorded, f.marks, log.lines)
	}
}

// The loop waits out what is left of the interval since the last pass: a
// restart does not look at every host again.
func TestRepositoryHostCheckLoopHonoursTheLastPass(t *testing.T) {
	run := func(f *fakeHostCheck, every time.Duration) (passes int, log *recLog) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		var probed []string
		log = &recLog{}
		f.onMark = cancel // one pass is enough
		c := &repositoryHostCheck{store: f, probe: probeAll(hostwatch.Clear, &probed), logger: log, every: every, on: true, configured: []string{"central.example"}, pause: time.Millisecond}
		c.run(ctx)
		return len(f.marks), log
	}
	if n, _ := run(&fakeHostCheck{}, time.Hour); n != 1 {
		t.Errorf("never checked before: %d passes; want one at once", n)
	}
	if n, _ := run(&fakeHostCheck{ever: true, since: 2 * time.Hour}, time.Hour); n != 1 {
		t.Errorf("last pass two intervals ago: %d passes; want one at once", n)
	}
	if n, _ := run(&fakeHostCheck{ever: true, since: time.Minute}, time.Hour); n != 0 {
		t.Errorf("last pass a minute ago, hourly: %d passes; want none yet", n)
	}
	// When the last pass ran cannot be read, that is not "never" (SR-5).
	if n, log := run(&fakeHostCheck{sinceErr: errors.New("timeout")}, time.Hour); n != 0 || !log.has("could not be read") {
		t.Errorf("unreadable mark: %d passes, log %v; want none, and the failure logged", n, log.lines)
	}
	// A pass whose mark fails rests an interval: it does not start over at once.
	f := &fakeHostCheck{markErr: errors.New("timeout")}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var probed []string
	(&repositoryHostCheck{store: f, probe: probeAll(hostwatch.Clear, &probed), logger: &recLog{}, every: time.Hour, on: true, configured: []string{"central.example"}, pause: time.Millisecond}).run(ctx)
	if len(probed) != 1 {
		t.Errorf("a pass that could not be marked ran %d times in a row; want once, then a rest", len(probed))
	}
}

// The knobs, end to end (SR-10): the file's value is the loop's interval,
// an explicit 0 turns the check off, and declared_repository_fetch decides
// whether declared hosts are contacted.
func TestRepositoryHostCheckFromTheConfigFile(t *testing.T) {
	load := func(collection string) *config.Config {
		t.Helper()
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(path, []byte(`{"collection": {`+collection+`}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(path)
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	var registrationAsked, threatListURLs, threatListHosts []string
	build := func(collection string, f *fakeHostCheck, probed *[]string, log *recLog) *repositoryHostCheck {
		c := newRepositoryHostCheck(f, probeAll(hostwatch.Clear, probed), log, func() func(context.Context, string, time.Time) hostwatch.Observation {
			return func(_ context.Context, host string, _ time.Time) hostwatch.Observation {
				registrationAsked = append(registrationAsked, host)
				return hostwatch.Observation{Kind: hostwatch.KindRegisteredLater, State: hostwatch.Clear}
			}
		}, func(listURL string) func(context.Context, []string) func(string) hostwatch.Observation {
			threatListURLs = append(threatListURLs, listURL)
			return func(_ context.Context, hosts []string) func(string) hostwatch.Observation {
				threatListHosts = append(threatListHosts, strings.Join(hosts, ","))
				return func(string) hostwatch.Observation {
					return hostwatch.Observation{Kind: hostwatch.KindThreatList, State: hostwatch.Clear}
				}
			}
		}, &load(collection).Collection)
		c.pause = time.Millisecond
		return c
	}
	// The registration check's own knob: on by default, off when set off.
	f2 := &fakeHostCheck{answered: []string{"declared.example"}}
	var p2 []string
	build(``, f2, &p2, &recLog{}).pass(context.Background())
	if strings.Join(registrationAsked, ",") != "declared.example" {
		t.Errorf("by default the registration check asked for %v; want the answered host", registrationAsked)
	}
	registrationAsked = nil
	build(`"repository_host_registration_check": false`, f2, &p2, &recLog{}).pass(context.Background())
	if len(registrationAsked) != 0 {
		t.Errorf("with repository_host_registration_check off, it asked for %v", registrationAsked)
	}
	// The threat list's setting: the default list when absent, the file's
	// URL when set, off when set empty; the list is fetched once a pass, for
	// every known host, a host that is never contacted included.
	threatListURLs, threatListHosts = nil, nil
	f3 := &fakeHostCheck{answered: []string{"declared.example"}}
	build(`"declared_repository_fetch": false, "repository_host_registration_check": false, "maven_repositories": ["https://mirror.example/m2"]`, f3, &p2, &recLog{}).pass(context.Background())
	if len(threatListURLs) != 1 || threatListURLs[0] != config.DefaultRepositoryHostThreatListURL {
		t.Errorf("by default the list is %v; want the default list", threatListURLs)
	}
	if len(threatListHosts) != 1 || threatListHosts[0] != "declared.example,mirror.example" {
		t.Errorf("the list was matched for %v; want once, for every known host", threatListHosts)
	}
	if o := f3.recorded["declared.example"]; len(o) != 1 || o[0].Kind != "threat_list" {
		t.Errorf("a host that is not contacted still gets the list's answer: %+v", o)
	}
	threatListURLs = nil
	build(`"repository_host_threat_list_url": "https://lists.example/hosts.txt"`, f3, &p2, &recLog{})
	if len(threatListURLs) != 1 || threatListURLs[0] != "https://lists.example/hosts.txt" {
		t.Errorf("a configured list: %v", threatListURLs)
	}
	threatListURLs, threatListHosts = nil, nil
	build(`"repository_host_threat_list_url": ""`, f3, &p2, &recLog{}).pass(context.Background())
	if len(threatListURLs) != 0 || len(threatListHosts) != 0 {
		t.Errorf("an empty setting must turn the signal off: %v %v", threatListURLs, threatListHosts)
	}
	for _, bad := range []string{`"repository_host_threat_list_url": "http://lists.example/hosts.txt"`, `"repository_host_threat_list_url": "https://user:pw@lists.example/hosts.txt"`, `"repository_host_threat_list_url": "lists.example"`} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		_ = os.WriteFile(path, []byte(`{"collection": {`+bad+`}}`), 0o600)
		if _, err := config.Load(path); err == nil {
			t.Errorf("%s must be refused at load", bad)
		}
	}
	var probed []string
	if c := build(`"repository_host_check_hours": 7`, &fakeHostCheck{}, &probed, &recLog{}); c.every != 7*time.Hour || !c.on {
		t.Errorf("7 hours: every %s on %v", c.every, c.on)
	}
	if c := build(``, &fakeHostCheck{}, &probed, &recLog{}); c.every != time.Duration(config.DefaultRepositoryHostCheckHours)*time.Hour || !c.on || !c.declared {
		t.Errorf("the defaults: every %s on %v declared %v", c.every, c.on, c.declared)
	}
	// 0 is off: the loop returns without contacting anything.
	log := &recLog{}
	f := &fakeHostCheck{answered: []string{"a.example"}}
	build(`"repository_host_check_hours": 0`, f, &probed, log).run(context.Background())
	if len(probed) != 0 || !log.has("is off") {
		t.Errorf("an explicit 0: probed %v, log %v; want the check off", probed, log.lines)
	}
	// The configured repositories are the ones looked at when declared
	// fetching is off.
	probed = nil
	c := build(`"declared_repository_fetch": false, "maven_repositories": ["https://mirror.example/m2"]`, &fakeHostCheck{answered: []string{"declared.example"}}, &probed, &recLog{})
	c.pass(context.Background())
	if strings.Join(probed, ",") != "mirror.example" {
		t.Errorf("declared_repository_fetch off with one configured mirror: probed %v", probed)
	}
	for _, bad := range []string{`"repository_host_check_hours": -1`} {
		path := filepath.Join(t.TempDir(), "aveloxis.json")
		_ = os.WriteFile(path, []byte(`{"collection": {`+bad+`}}`), 0o600)
		if _, err := config.Load(path); err == nil {
			t.Errorf("%s must be refused at load", bad)
		}
	}
}

// The registration-date check asks registries, never the host: it covers
// every host that has answered even with declared fetching off, skips a
// host that never answered (nothing to compare with) and a distrusted one,
// and gets a fresh check each pass.
func TestRepositoryHostCheckRegistrationDates(t *testing.T) {
	f := &fakeHostCheck{answered: []string{"declared.example", "central.example", "gone.example"}, distrusted: []string{"gone.example"}}
	var probed, asked []string
	made := 0
	c := &repositoryHostCheck{store: f, probe: probeAll(hostwatch.Clear, &probed), logger: &recLog{}, every: time.Hour, on: true,
		configured: []string{"central.example", "never-answered.example"}, declared: false, pause: time.Millisecond,
		registration: func() func(context.Context, string, time.Time) hostwatch.Observation {
			made++
			return func(_ context.Context, host string, first time.Time) hostwatch.Observation {
				if !first.Equal(fakeFirstFetch) {
					t.Errorf("%s: first fetch %s", host, first)
				}
				asked = append(asked, host)
				return hostwatch.Observation{Kind: hostwatch.KindRegisteredLater, State: hostwatch.Signal, Detail: "later"}
			}
		}}
	c.pass(context.Background())
	c.pass(context.Background())
	if made != 2 {
		t.Errorf("%d checks made for two passes; each pass asks afresh", made)
	}
	if got := strings.Join(asked[:len(asked)/2], ","); got != "central.example,declared.example" {
		t.Errorf("registration asked for %q; want every answered host that is not distrusted, and not one that never answered", got)
	}
	if got := strings.Join(probed[:len(probed)/2], ","); got != "central.example,never-answered.example" {
		t.Errorf("contacted %q; with declared fetching off only the configured hosts are contacted", got)
	}
	// The declared host was not contacted, and its registration answer is stored.
	if o := f.recorded["declared.example"]; len(o) != 1 || o[0].Kind != "registered_later" || o[0].State != "signal" {
		t.Errorf("the declared host's record: %+v", o)
	}
	if o := f.recorded["central.example"]; len(o) != 2 {
		t.Errorf("the configured host's record: %+v; want the probe's answer and the registration's", o)
	}
	// Off: nothing is asked, and a host that is only "answered" is not in the pass.
	c.registration, asked, f.recorded = nil, nil, nil
	c.pass(context.Background())
	if len(asked) != 0 || f.recorded["declared.example"] != nil {
		t.Errorf("with the registration check off: asked %v, recorded %v", asked, f.recorded)
	}
}
