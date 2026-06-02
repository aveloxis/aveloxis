// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package mailinglist implements pluggable mailing-list collection
// (v0.26.0). A System pairs an archive backend (how to read the archive)
// with an ordered classification ruleset (which addresses/subjects map to
// which message class). Both are configuration so a new mailing-list
// system — Apache Pony Mail, lore.kernel.org public-inbox, future
// Mailman/Hyperkitty — is added by editing systems.yaml, not Go code.
package mailinglist

import (
	_ "embed"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

//go:embed systems.yaml
var systemsYAML []byte

// Classification message classes (Axis A — §3a of the design).
const (
	ClassIssueEvent      = "issue_event"
	ClassGitHubMirror    = "github_mirror"
	ClassCommitNotify    = "commit_notify"
	ClassPatchSubmission = "patch_submission"
	ClassReview          = "review"
	ClassVote            = "vote"
	ClassAnnounce        = "announce"
	ClassResult          = "result"
	ClassDiscuss         = "discuss"
	ClassSupport         = "support"
	ClassUnclassified    = "unclassified"
)

// Matcher is the per-field regex set of a rule. A rule matches when every
// non-empty field matches its corresponding input. Empty fields are ignored.
type Matcher struct {
	ListID      string `yaml:"list_id"`
	ListAddress string `yaml:"list_address"`
	Subject     string `yaml:"subject"`
	Sender      string `yaml:"sender"`
	Body        string `yaml:"body"`
}

// Rule maps a matcher to a class plus optional named capture groups. Capture
// values are the regex submatch indices of whichever matcher field carries
// the groups (priority: subject > body > sender > list_id > list_address).
type Rule struct {
	Match   Matcher        `yaml:"match"`
	Class   string         `yaml:"class"`
	Capture map[string]int `yaml:"capture"`
}

// System is one mailing-list system definition.
type System struct {
	Name            string `yaml:"name"`
	ArchiveBackend  string `yaml:"archive_backend"`
	BaseURL         string `yaml:"base_url"`
	RepoURLTemplate string `yaml:"repo_url_template"` // e.g. https://github.com/apache/{repo}
	Rules           []Rule `yaml:"rules"`

	compiled []compiledRule // built by compile()
}

type compiledRule struct {
	rule                                    Rule
	listID, listAddr, subject, sender, body *regexp.Regexp
}

// Message is the minimal view of an email the classifier needs.
type Message struct {
	ListID      string // List-Id header value, e.g. "<dev.kafka.apache.org>"
	ListAddress string // the list address, e.g. "dev@kafka.apache.org"
	Subject     string
	Sender      string // From header
	Body        string
}

// Classification is the result of classifying one message.
type Classification struct {
	Class    string
	Source   string            // subject_regex | body_url | sender | list_id | list_address | unclassified
	Captures map[string]string // e.g. {"external_key": "KAFKA-20167", "repo": "arrow-rs"}
}

type catalog struct {
	Systems []System `yaml:"systems"`
}

// LoadSystems parses the embedded systems.yaml and returns a registry keyed
// by system name, with every rule's regexes precompiled. A bad regex in the
// YAML fails fast here rather than at classify time.
func LoadSystems() (map[string]*System, error) {
	return parseSystems(systemsYAML)
}

func parseSystems(data []byte) (map[string]*System, error) {
	var c catalog
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse systems.yaml: %w", err)
	}
	out := make(map[string]*System, len(c.Systems))
	for i := range c.Systems {
		sys := &c.Systems[i]
		if err := sys.compile(); err != nil {
			return nil, fmt.Errorf("system %q: %w", sys.Name, err)
		}
		out[sys.Name] = sys
	}
	return out, nil
}

func (s *System) compile() error {
	s.compiled = s.compiled[:0]
	for _, r := range s.Rules {
		cr := compiledRule{rule: r}
		var err error
		if cr.listID, err = compileField(r.Match.ListID); err != nil {
			return err
		}
		if cr.listAddr, err = compileField(r.Match.ListAddress); err != nil {
			return err
		}
		if cr.subject, err = compileField(r.Match.Subject); err != nil {
			return err
		}
		if cr.sender, err = compileField(r.Match.Sender); err != nil {
			return err
		}
		if cr.body, err = compileField(r.Match.Body); err != nil {
			return err
		}
		s.compiled = append(s.compiled, cr)
	}
	return nil
}

func compileField(pat string) (*regexp.Regexp, error) {
	if pat == "" {
		return nil, nil
	}
	return regexp.Compile(pat)
}

// Classify returns the class + capture groups for a message. First matching
// rule wins; a catch-all `match: {}` rule should be last as the default.
// If nothing matches, returns ClassUnclassified.
func (s *System) Classify(m Message) Classification {
	for _, cr := range s.compiled {
		subM := fieldMatch(cr.subject, m.Subject)
		bodyM := fieldMatch(cr.body, m.Body)
		sendM := fieldMatch(cr.sender, m.Sender)
		lidM := fieldMatch(cr.listID, m.ListID)
		laM := fieldMatch(cr.listAddr, m.ListAddress)
		if subM == nil || bodyM == nil || sendM == nil || lidM == nil || laM == nil {
			continue // some specified field failed
		}
		// All specified fields matched. Determine source + captures from the
		// highest-priority field that actually carried a regex.
		source, sub := classifySource(cr, subM, bodyM, sendM, lidM, laM)
		return Classification{
			Class:    cr.rule.Class,
			Source:   source,
			Captures: extractCaptures(cr.rule.Capture, sub),
		}
	}
	return Classification{Class: ClassUnclassified, Source: "unclassified"}
}

// fieldMatch returns the submatch slice when re matches input, a non-nil
// empty marker when re is nil (field not specified → vacuously matches), or
// nil when re is specified but does not match.
func fieldMatch(re *regexp.Regexp, input string) []string {
	if re == nil {
		return []string{} // vacuously matches; no captures
	}
	if m := re.FindStringSubmatch(input); m != nil {
		return m
	}
	return nil
}

// classifySource picks the source label + the submatch slice to read
// captures from, by field priority.
func classifySource(cr compiledRule, subM, bodyM, sendM, lidM, laM []string) (string, []string) {
	switch {
	case cr.subject != nil:
		return "subject_regex", subM
	case cr.body != nil:
		return "body_url", bodyM
	case cr.sender != nil:
		return "sender", sendM
	case cr.listID != nil:
		return "list_id", lidM
	case cr.listAddr != nil:
		return "list_address", laM
	default:
		return "unclassified", nil // catch-all rule
	}
}

func extractCaptures(capture map[string]int, sub []string) map[string]string {
	if len(capture) == 0 || len(sub) == 0 {
		return nil
	}
	out := make(map[string]string, len(capture))
	for name, idx := range capture {
		if idx >= 0 && idx < len(sub) {
			out[name] = sub[idx]
		}
	}
	return out
}

// RepoURLFromCaptures synthesizes the canonical repo URL a message signals,
// per §5c. Prefers an explicit owner/repo (from a body github URL) over the
// system's repo-URL template applied to a bare [repo] tag. Returns "" when
// there's no usable repo signal (e.g. a soft component tag like "[RUST]").
func (s *System) RepoURLFromCaptures(c Classification) string {
	owner := c.Captures["owner"]
	repo := c.Captures["repo"]
	if owner != "" && repo != "" {
		return "https://github.com/" + owner + "/" + strings.TrimSuffix(repo, ".git")
	}
	if repo != "" && s.RepoURLTemplate != "" {
		return strings.ReplaceAll(s.RepoURLTemplate, "{repo}", repo)
	}
	return ""
}
