// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package jira is the minimal Jira Server REST client (C2 of the email
// attribution program). Deliberately NOT a platform.Client — Jira
// projects map onto repos we already track, so the subsystem follows
// the mailing-list registration pattern, not the repo-platform one.
//
// Scope facts (measured against issues.apache.org, 2026-08-31):
// self-hosted Server 8.20.10, anonymous reads, /rest/api/2/search
// honors maxResults=1000 and returns FULL comment bodies inline, no
// rate-limit headers or 429s observed across ~700 requests — limits
// are self-imposed politeness, which belongs to the CALLER (worker /
// CLI pacing + breaker), not here. Identity is the Server-era stable
// `name` field; Atlassian Cloud's API removed it (GDPR, 2019), which
// is the collection-window risk that motivates banking identities
// early.
package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// ErrInvalidQuery is the TYPE sentinel for a 400 from the search
// endpoint (`errors.Is` matches any *invalidQueryError regardless of
// kind). Copilot round 23: a 400 is NOT automatically a dead key — only
// a "project does not exist" 400 (deadKey) classifies ClassSkip and
// disables the project (the 5 dead pilot keys); a JQL field/compat 400
// classifies ClassFatal so the worker records a failure without
// disabling every registered project.
var ErrInvalidQuery = &invalidQueryError{}

type invalidQueryError struct {
	detail  string
	deadKey bool // Copilot round 23: only a "project does not exist" 400 disables
}

func (e *invalidQueryError) Error() string {
	if e.detail == "" {
		return "jira: invalid query"
	}
	return "jira: invalid query: " + e.detail
}
func (e *invalidQueryError) Is(target error) bool {
	_, ok := target.(*invalidQueryError)
	return ok
}
func (e *invalidQueryError) Class() platform.ErrorClass {
	// Copilot round 23 (PR #193): ONLY a "project does not exist" 400 is a
	// dead key the worker should DISABLE. Jira also returns 400 for
	// malformed/unsupported JQL fields and other request errors — a field
	// compatibility issue must NOT disable every registered project.
	// Non-dead 400s classify ClassFatal: the worker records a failure
	// (surfacing it for the operator) without disabling.
	if e.deadKey {
		return platform.ClassSkip
	}
	return platform.ClassFatal
}

// jiraProjectNotFound reports whether a Jira 400 body is the
// project-does-not-exist error (Server 8.20: "The value 'X' does not
// exist for the field 'project'."), as opposed to a JQL field or other
// request error.
func jiraProjectNotFound(detail string) bool {
	return strings.Contains(strings.ToLower(detail), "does not exist for the field 'project'")
}

// rateLimitError classifies a 429 for the subdividing/pacing callers.
type rateLimitError struct{}

func (rateLimitError) Error() string              { return "jira: rate limited (429)" }
func (rateLimitError) Class() platform.ErrorClass { return platform.ClassRateLimit }

// User is a Jira Server identity. Name is the stable username (the
// matching key); Key is the internal JIRAUSERnnnn identifier;
// EmailAddress is empty for anonymous API access (verified live).
type User struct {
	Name         string `json:"name"`
	Key          string `json:"key"`
	DisplayName  string `json:"displayName"`
	EmailAddress string `json:"emailAddress"`
}

// Named is a {name} object (status, resolution).
type Named struct {
	Name string `json:"name"`
}

// Comment is one issue comment; ID is Jira's numeric comment id (the
// platform_msg_id for the native messages row).
type Comment struct {
	ID      string `json:"id"`
	Author  *User  `json:"author"`
	Body    string `json:"body"`
	Created string `json:"created"`
	Updated string `json:"updated"`
}

// CommentBlock is the inline comments connection. The pilot measured
// zero truncation on 200 issues / 718 comments. When Jira DOES cap
// the block (Total > len(Comments)), the WORKER completes it before
// staging — it re-walks IssueCommentsPage and REPLACES the block
// (Copilot round 2 on PR #193, #2); the processor's Total-vs-len WARN
// is only the backstop for envelopes staged before that fix.
type CommentBlock struct {
	Total    int       `json:"total"`
	Comments []Comment `json:"comments"`
}

// IssueFields is the field selection this subsystem uses.
type IssueFields struct {
	Summary        string        `json:"summary"`
	Reporter       *User         `json:"reporter"`
	Assignee       *User         `json:"assignee"`
	Status         *Named        `json:"status"`
	Resolution     *Named        `json:"resolution"`
	ResolutionDate string        `json:"resolutiondate"`
	Created        string        `json:"created"`
	Updated        string        `json:"updated"`
	Comment        *CommentBlock `json:"comment"`
}

// Issue is one search result. ID is Jira's internal numeric id (as a
// string on the wire) — stored in issues.jira_issue_id; the synthetic
// negative platform_issue_id NEVER changes (C3a).
type Issue struct {
	ID     string      `json:"id"`
	Key    string      `json:"key"`
	Fields IssueFields `json:"fields"`
}

// SearchResult is one /rest/api/2/search page.
type SearchResult struct {
	StartAt    int     `json:"startAt"`
	MaxResults int     `json:"maxResults"`
	Total      int     `json:"total"`
	Issues     []Issue `json:"issues"`
}

// Client is a dumb typed HTTP wrapper. One instance per base URL.
type Client struct {
	baseURL   string
	userAgent string
	http      *http.Client
}

// New builds a client for a Jira Server base URL (e.g.
// https://issues.apache.org/jira). politeEmail lands in the User-Agent
// so the instance's admins can reach us (the ecosyste.ms polite
// pattern).
func New(baseURL, politeEmail string) *Client {
	ua := "aveloxis (+https://github.com/aveloxis/aveloxis"
	if politeEmail != "" {
		ua += "; " + politeEmail
	}
	ua += ")"
	return &Client{
		baseURL:   strings.TrimRight(baseURL, "/"),
		userAgent: ua,
		http: &http.Client{
			Timeout: 120 * time.Second, // a 1000-row page with comments measured 8.5s; headroom, not budget
		},
	}
}

// SearchPage runs one /rest/api/2/search page. fields nil = server
// default; pass the explicit selection in production paths.
func (c *Client) SearchPage(ctx context.Context, jql string, fields []string, startAt, maxResults int) (*SearchResult, error) {
	q := url.Values{}
	q.Set("jql", jql)
	if len(fields) > 0 {
		q.Set("fields", strings.Join(fields, ","))
	}
	q.Set("startAt", strconv.Itoa(startAt))
	q.Set("maxResults", strconv.Itoa(maxResults))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/rest/api/2/search?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("jira search request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jira search: %w: %w", platform.ErrTransient, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		// fall through to decode
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, fmt.Errorf("jira search: %w", rateLimitError{})
	case resp.StatusCode == http.StatusBadRequest:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		detail := string(body)
		return nil, fmt.Errorf("jira search: %w", &invalidQueryError{detail: detail, deadKey: jiraProjectNotFound(detail)})
	case resp.StatusCode >= 500:
		return nil, fmt.Errorf("jira search: status %d: %w", resp.StatusCode, platform.ErrTransient)
	default:
		return nil, fmt.Errorf("jira search: unexpected status %d", resp.StatusCode)
	}

	var out SearchResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("jira search decode: %w: %w", platform.ErrTransient, err)
	}
	return &out, nil
}

// CommentPage is one /rest/api/2/issue/{key}/comment page.
type CommentPage struct {
	StartAt    int       `json:"startAt"`
	MaxResults int       `json:"maxResults"`
	Total      int       `json:"total"`
	Comments   []Comment `json:"comments"`
}

// IssueCommentsPage fetches one page of an issue's comments from the
// dedicated comment endpoint. Used ONLY when a search result's inline
// comment block is truncated (Fields.Comment.Total > len(Comments)) —
// Jira caps the embedded block independently of the search page size,
// and an envelope staged with a truncated block would lose the tail
// forever (Copilot round 2 on PR #193, #2). Error classes mirror
// SearchPage so ClassifyError routing is identical.
func (c *Client) IssueCommentsPage(ctx context.Context, issueKey string, startAt, maxResults int) (*CommentPage, error) {
	q := url.Values{}
	q.Set("startAt", strconv.Itoa(startAt))
	q.Set("maxResults", strconv.Itoa(maxResults))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/rest/api/2/issue/"+url.PathEscape(issueKey)+"/comment?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("jira comments request: %w", err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jira comments: %w: %w", platform.ErrTransient, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK:
		// fall through to decode
	case resp.StatusCode == http.StatusTooManyRequests:
		return nil, fmt.Errorf("jira comments: %w", rateLimitError{})
	case resp.StatusCode >= 500:
		return nil, fmt.Errorf("jira comments: status %d: %w", resp.StatusCode, platform.ErrTransient)
	default:
		return nil, fmt.Errorf("jira comments: unexpected status %d", resp.StatusCode)
	}

	var out CommentPage
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("jira comments decode: %w: %w", platform.ErrTransient, err)
	}
	return &out, nil
}

// ProjectTotal returns the issue count of a project (one maxResults=0
// search — ~0.4s measured). A project-not-found 400 = dead project key (disable); other 400s surface without disabling (round 23).
func (c *Client) ProjectTotal(ctx context.Context, projectKey string) (int, error) {
	// Copilot round 27 (PR #193): same JQL-injection guard as the walk.
	if !ValidProjectKey(projectKey) {
		return 0, fmt.Errorf("refusing to count malformed Jira project key %q (want [A-Z][A-Z0-9]+)", projectKey)
	}
	res, err := c.SearchPage(ctx, `project = "`+projectKey+`"`, nil, 0, 0)
	if err != nil {
		return 0, err
	}
	return res.Total, nil
}
