// Package collector — facade.go implements git-based commit collection.
//
// This is the equivalent of Augur's "facade worker". It clones a repository
// (or fetches updates), then parses `git log` output to populate the commits,
// commit_parents, and commit_messages tables.
//
// The facade phase runs AFTER the API phases (issues, PRs, events, messages)
// because API data is needed first for contributor resolution of commit emails.
package collector

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/model"
)

// FacadeCollector handles git clone/fetch + log parsing for commit data.
type FacadeCollector struct {
	store        *db.PostgresStore
	logger       *slog.Logger
	repoDir      string // base directory for cloned repos
	affiliations *db.AffiliationResolver
}

// NewFacadeCollector creates a facade collector. repoDir is the base directory
// where repos will be cloned (e.g., /tmp/aveloxis-repos).
func NewFacadeCollector(store *db.PostgresStore, logger *slog.Logger, repoDir string) *FacadeCollector {
	return &FacadeCollector{
		store:        store,
		logger:       logger,
		repoDir:      repoDir,
		affiliations: db.NewAffiliationResolver(store),
	}
}

// FacadeResult summarizes the outcome of a facade collection run.
type FacadeResult struct {
	Commits        int
	CommitMessages int
	Errors         []error
}

// CollectRepo clones (or fetches) the repo and parses git log for commit data.
func (f *FacadeCollector) CollectRepo(ctx context.Context, repoID int64, gitURL string) (*FacadeResult, error) {
	result := &FacadeResult{}

	// Determine local clone path.
	clonePath := f.clonePath(repoID)

	// Clone or fetch.
	if err := f.ensureClone(ctx, gitURL, clonePath); err != nil {
		return result, fmt.Errorf("clone/fetch: %w", err)
	}

	// Parse git log.
	if err := f.parseGitLog(ctx, repoID, clonePath, result); err != nil {
		return result, fmt.Errorf("git log: %w", err)
	}

	// Refresh aggregates (dm_repo_annual/monthly/weekly).
	f.logger.Info("refreshing aggregates", "repo_id", repoID)
	if err := f.store.RefreshRepoAggregates(ctx, repoID); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("repo aggregates: %w", err))
	}
	if err := f.store.RefreshRepoGroupAggregates(ctx, repoID); err != nil {
		result.Errors = append(result.Errors, fmt.Errorf("repo group aggregates: %w", err))
	}

	return result, nil
}

func (f *FacadeCollector) clonePath(repoID int64) string {
	return filepath.Join(f.repoDir, fmt.Sprintf("repo_%d", repoID))
}

// ensureClone either fetches updates for an existing bare clone or creates a new one.
func (f *FacadeCollector) ensureClone(ctx context.Context, gitURL, path string) error {
	// Bare repos don't have a .git subdirectory — check for HEAD file instead.
	if _, err := os.Stat(filepath.Join(path, "HEAD")); err == nil {
		// Existing bare clone found — verify it's the right repo.
		// When repo IDs are reassigned (e.g., new database pointing at the same
		// clone directory), the old clone may belong to a completely different repo.
		// Reusing it would parse the wrong git history.
		configData, _ := os.ReadFile(filepath.Join(path, "config"))
		existingURL := parseOriginURL(string(configData))
		if existingURL != "" && normalizeCloneURL(existingURL) != normalizeCloneURL(gitURL) {
			f.logger.Warn("stale clone detected: origin URL mismatch, re-cloning",
				"path", path,
				"existing_url", existingURL,
				"expected_url", gitURL)
			os.RemoveAll(path)
			return f.freshClone(ctx, gitURL, path)
		}

		f.logger.Info("fetching updates", "path", path)
		var stderr bytes.Buffer
		cmd := exec.CommandContext(ctx, "git", "-C", path, "fetch", "--all", "--prune")
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			f.logger.Warn("fetch failed, re-cloning",
				"path", path, "error", err, "stderr", stderr.String())
			os.RemoveAll(path)
			return f.freshClone(ctx, gitURL, path)
		}
		return nil
	}

	// If the directory exists but isn't a valid repo (interrupted clone), remove it.
	if _, err := os.Stat(path); err == nil {
		f.logger.Warn("removing corrupt/incomplete clone directory", "path", path)
		os.RemoveAll(path)
	}

	return f.freshClone(ctx, gitURL, path)
}

// parseOriginURL extracts the remote.origin.url from a git config file's content.
func parseOriginURL(configContent string) string {
	inOrigin := false
	for _, line := range strings.Split(configContent, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == `[remote "origin"]` {
			inOrigin = true
			continue
		}
		if inOrigin {
			if strings.HasPrefix(trimmed, "[") {
				break // next section
			}
			if strings.HasPrefix(trimmed, "url = ") {
				return strings.TrimPrefix(trimmed, "url = ")
			}
		}
	}
	return ""
}

// normalizeCloneURL strips protocol, trailing slashes, .git suffix, and lowercases
// for URL comparison. Two URLs that point to the same repo should match after this.
func normalizeCloneURL(u string) string {
	u = strings.TrimPrefix(u, "https://")
	u = strings.TrimPrefix(u, "http://")
	u = strings.TrimPrefix(u, "git://")
	u = strings.TrimPrefix(u, "ssh://")
	u = strings.TrimSuffix(u, "/")
	u = strings.TrimSuffix(u, ".git")
	return strings.ToLower(u)
}

func (f *FacadeCollector) freshClone(ctx context.Context, gitURL, path string) error {
	if err := validateGitURL(gitURL); err != nil {
		return fmt.Errorf("unsafe git URL rejected: %w", err)
	}
	f.logger.Info("cloning repository", "url", gitURL, "path", path)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "git", "clone", "--bare", "--", gitURL, path)
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, stderr.String())
	}
	return nil
}

// validateGitURL checks that a URL is safe to pass to git commands.
// Rejects URLs that could be interpreted as git flags (start with "-"),
// non-network schemes (file://), and URLs containing control characters.
func validateGitURL(u string) error {
	if u == "" {
		return fmt.Errorf("empty URL")
	}
	// Reject flag injection: URLs starting with "-" could be parsed as git flags.
	if u[0] == '-' {
		return fmt.Errorf("URL must not start with '-': %q", u)
	}
	// Reject control characters (newlines, null bytes) that could break argument parsing.
	for _, c := range u {
		if c < 0x20 || c == 0x7f {
			return fmt.Errorf("URL contains control character 0x%02x", c)
		}
	}
	// Require a recognized scheme or SCP-style SSH syntax (git@host:path).
	// Allowed schemes: https, http, git, ssh.
	// SCP syntax: contains "@" and ":" but no "://" (e.g., git@github.com:org/repo.git).
	allowedSchemes := []string{"https://", "http://", "git://", "ssh://"}
	for _, scheme := range allowedSchemes {
		if strings.HasPrefix(u, scheme) {
			return nil
		}
	}
	// SCP-style: user@host:path
	if strings.Contains(u, "@") && strings.Contains(u, ":") && !strings.Contains(u, "://") {
		return nil
	}
	return fmt.Errorf("unrecognized URL scheme (allowed: https, http, git, ssh, or SCP-style): %q", u)
}

// gitLogFormat is the format string for git log output.
// Fields are separated by the record separator character (0x1e).
// Each commit is terminated by the group separator (0x1d).
const (
	fieldSep  = "\x1e"
	commitSep = "\x1d"
	// Format: hash, parent_hashes, author_name, author_email, author_date,
	//         committer_name, committer_email, committer_date, subject
	gitLogFormat = "%H" + fieldSep + "%P" + fieldSep +
		"%an" + fieldSep + "%ae" + fieldSep + "%aI" + fieldSep +
		"%cn" + fieldSep + "%ce" + fieldSep + "%cI" + fieldSep +
		"%s" + commitSep
)

// parseGitLog runs git log and inserts commits into the database.
func (f *FacadeCollector) parseGitLog(ctx context.Context, repoID int64, clonePath string, result *FacadeResult) error {
	f.logger.Info("parsing git log", "repo_id", repoID, "path", clonePath)

	// Determine the default branch (what HEAD points to in the bare clone).
	// Only count commits on the default branch (main/master/dev/etc.) to match
	// the GitHub/GitLab API commit count. Augur does the same — --all would
	// include unmerged feature branches and inflate the count.
	defaultBranch := resolveDefaultBranch(ctx, clonePath)
	f.logger.Info("using default branch for git log", "repo_id", repoID, "branch", defaultBranch)

	// Run git log with --numstat for per-file stats on the default branch only.
	cmd := exec.CommandContext(ctx, "git", "-C", clonePath, "log",
		defaultBranch,
		"--numstat",
		"--format="+gitLogFormat,
	)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("starting git log: %w", err)
	}

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 1024*1024), 10*1024*1024)

	var (
		currentCommit *parsedCommit
		batch         []*parsedCommit
	)
	const batchSize = 500

	for scanner.Scan() {
		line := scanner.Text()

		// Check if this line contains a commit record separator.
		// Format: HASH\x1ePARENTS\x1e...\x1eSUBJECT\x1d  (separator at END of header)
		if strings.Contains(line, commitSep) {
			parts := strings.SplitN(line, commitSep, 2)

			// Flush previous commit before starting a new one.
			if currentCommit != nil {
				batch = append(batch, currentCommit)
				if len(batch) >= batchSize {
					if err := f.insertCommitBatch(ctx, repoID, batch, result); err != nil {
						result.Errors = append(result.Errors, err)
					}
					batch = batch[:0]
				}
			}

			// The commit header is BEFORE the separator (parts[0]).
			header := parts[0]
			if strings.Contains(header, fieldSep) {
				currentCommit = parseCommitHeader(header)
			} else {
				currentCommit = nil
			}

			// If there's content after the separator, it's the next commit header
			// (happens when two commits have no numstat lines between them).
			if len(parts) > 1 && parts[1] != "" && strings.Contains(parts[1], fieldSep) {
				// Flush the commit we just parsed (it has no files).
				if currentCommit != nil {
					batch = append(batch, currentCommit)
				}
				currentCommit = parseCommitHeader(parts[1])
			}
			continue
		}

		// Check if this is a commit header line (first line of output or after blank).
		if strings.Contains(line, fieldSep) && strings.Count(line, fieldSep) >= 7 {
			if currentCommit != nil {
				batch = append(batch, currentCommit)
				if len(batch) >= batchSize {
					if err := f.insertCommitBatch(ctx, repoID, batch, result); err != nil {
						result.Errors = append(result.Errors, err)
					}
					batch = batch[:0]
				}
			}
			currentCommit = parseCommitHeader(line)
			continue
		}

		// Numstat line (or blank line).
		if currentCommit != nil && line != "" {
			parseNumstatLine(currentCommit, line)
		}
	}

	// Flush last commit + remaining batch.
	if currentCommit != nil {
		batch = append(batch, currentCommit)
	}
	if len(batch) > 0 {
		if err := f.insertCommitBatch(ctx, repoID, batch, result); err != nil {
			result.Errors = append(result.Errors, err)
		}
	}

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git log exited with error: %w", err)
	}

	return scanner.Err()
}

type parsedCommit struct {
	Hash           string
	Parents        []string
	AuthorName     string
	AuthorEmail    string
	AuthorDate     string
	CommitterName  string
	CommitterEmail string
	CommitterDate  string
	Subject        string
	Files          []parsedFile
}

type parsedFile struct {
	Additions int
	Deletions int
	Filename  string
}

func parseCommitHeader(line string) *parsedCommit {
	// Remove any trailing commit separator.
	line = strings.TrimSuffix(line, commitSep)
	line = strings.TrimSpace(line)

	fields := strings.SplitN(line, fieldSep, 9)
	if len(fields) < 9 {
		return nil
	}
	var parents []string
	if fields[1] != "" {
		parents = strings.Fields(fields[1])
	}
	return &parsedCommit{
		Hash:           fields[0],
		Parents:        parents,
		AuthorName:     fields[2],
		AuthorEmail:    fields[3],
		AuthorDate:     fields[4],
		CommitterName:  fields[5],
		CommitterEmail: fields[6],
		CommitterDate:  fields[7],
		Subject:        fields[8],
	}
}

func parseNumstatLine(c *parsedCommit, line string) {
	parts := strings.SplitN(line, "\t", 3)
	if len(parts) != 3 {
		return
	}
	adds, _ := strconv.Atoi(parts[0])
	dels, _ := strconv.Atoi(parts[1])
	c.Files = append(c.Files, parsedFile{
		Additions: adds,
		Deletions: dels,
		Filename:  parts[2],
	})
}

// insertCommitBatch inserts a batch of parsed commits into the database.
func (f *FacadeCollector) insertCommitBatch(ctx context.Context, repoID int64, batch []*parsedCommit, result *FacadeResult) error {
	for _, pc := range batch {
		now := time.Now()
		// If there are no per-file entries, insert a single row for the commit.
		files := pc.Files
		if len(files) == 0 {
			files = []parsedFile{{Filename: "(no files)"}}
		}

		// Resolve affiliations from email domains.
		authorAff := f.affiliations.Resolve(ctx, pc.AuthorEmail)
		committerAff := f.affiliations.Resolve(ctx, pc.CommitterEmail)

		for _, file := range files {
			commit := &model.Commit{
				RepoID:               repoID,
				Hash:                 pc.Hash,
				AuthorName:           pc.AuthorName,
				AuthorRawEmail:       pc.AuthorEmail,
				AuthorEmail:          pc.AuthorEmail,
				AuthorDate:           extractDate(pc.AuthorDate),
				AuthorAffiliation:    authorAff,
				AuthorTimestamp:      parseTimestamp(pc.AuthorDate),
				CommitterName:        pc.CommitterName,
				CommitterRawEmail:    pc.CommitterEmail,
				CommitterEmail:       pc.CommitterEmail,
				CommitterDate:        extractDate(pc.CommitterDate),
				CommitterAffiliation: committerAff,
				CommitterTimestamp:   parseTimestamp(pc.CommitterDate),
				Filename:             file.Filename,
				LinesAdded:           file.Additions,
				LinesRemoved:         file.Deletions,
				Origin:               model.DataOrigin{ToolSource: "aveloxis-facade", DataSource: "git"},
			}
			commit.Origin.DataCollectionDate = now
			if err := f.store.UpsertCommit(ctx, commit); err != nil {
				f.logger.Warn("failed to upsert commit", "hash", pc.Hash, "file", file.Filename, "error", err)
				continue
			}
			result.Commits++
		}

		// Insert commit parents.
		for _, parentHash := range pc.Parents {
			if err := f.store.InsertCommitParent(ctx, repoID, pc.Hash, parentHash); err != nil {
				f.logger.Warn("failed to insert commit parent",
					"hash", pc.Hash, "parent", parentHash, "error", err)
			}
		}

		// Insert commit message.
		if pc.Subject != "" {
			msg := &model.CommitMessage{
				RepoID:  repoID,
				Hash:    pc.Hash,
				Message: pc.Subject,
			}
			if err := f.store.UpsertCommitMessage(ctx, msg); err != nil {
				f.logger.Warn("failed to upsert commit message", "hash", pc.Hash, "error", err)
			} else {
				result.CommitMessages++
			}
		}
	}
	return nil
}

// extractDate returns the YYYY-MM-DD portion of an ISO 8601 date string.
func extractDate(isoDate string) string {
	if len(isoDate) >= 10 {
		return isoDate[:10]
	}
	return isoDate
}

func parseTimestamp(isoDate string) *time.Time {
	t, err := time.Parse(time.RFC3339, isoDate)
	if err != nil {
		return nil
	}
	return &t
}

// resolveDefaultBranch returns the default branch name for a bare clone.
// In a bare repo, HEAD is a symbolic ref pointing to the default branch
// (e.g., "ref: refs/heads/main"). Falls back to "HEAD" if detection fails,
// which makes git log use whatever HEAD resolves to.
func resolveDefaultBranch(ctx context.Context, clonePath string) string {
	cmd := exec.CommandContext(ctx, "git", "-C", clonePath, "symbolic-ref", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "HEAD" // fallback — git log HEAD works on bare clones
	}
	// Output is like "refs/heads/main\n" — extract the branch name.
	ref := strings.TrimSpace(string(out))
	// Return the full ref so git log can use it directly.
	return ref
}
