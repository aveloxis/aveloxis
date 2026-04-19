// Package config handles Aveloxis configuration.
package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func defaultCloneDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "/tmp/aveloxis-repos"
	}
	return filepath.Join(home, "aveloxis-repos")
}

// Config is the top-level Aveloxis configuration.
type Config struct {
	Database DatabaseConfig `json:"database"`
	GitHub   PlatformConfig `json:"github"`
	GitLab   PlatformConfig `json:"gitlab"`

	// Collection controls how repositories are collected.
	Collection CollectionConfig `json:"collection"`

	// Web GUI settings.
	Web WebConfig `json:"web"`

	// LogLevel sets the minimum log level: "debug", "info", "warn", or "error".
	LogLevel string `json:"log_level"`
}

// DatabaseConfig holds PostgreSQL connection details.
type DatabaseConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	DBName   string `json:"dbname"`
	SSLMode  string `json:"sslmode"`
}

// ConnectionString returns a PostgreSQL DSN.
func (d DatabaseConfig) ConnectionString() string {
	sslmode := d.SSLMode
	if sslmode == "" {
		sslmode = "prefer"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		d.User, d.Password, d.Host, d.Port, d.DBName, sslmode)
}

// PlatformConfig holds API keys and settings for a forge platform.
type PlatformConfig struct {
	APIKeys []string `json:"api_keys"`
	BaseURL string   `json:"base_url,omitempty"` // override for self-hosted instances

	// GitLabHosts lists additional hostnames that should be recognized as
	// GitLab instances (for self-hosted). Only relevant for GitLab config.
	GitLabHosts []string `json:"gitlab_hosts,omitempty"`
}

// WebConfig configures the web GUI and OAuth.
type WebConfig struct {
	// Addr is the listen address for the web GUI (default ":8082").
	Addr string `json:"addr"`

	// SessionSecret is used to sign session cookies (generate a random string).
	SessionSecret string `json:"session_secret"`

	// BaseURL is the external URL for OAuth callbacks (e.g., "https://aveloxis.example.com").
	BaseURL string `json:"base_url"`

	// DevMode disables the Secure flag on cookies, allowing the web GUI to work
	// over plain HTTP during local development. In production (the default),
	// cookies are always marked Secure so browsers only send them over HTTPS.
	// HttpOnly is always set regardless of this flag.
	DevMode bool `json:"dev_mode"`

	// GitHub OAuth app credentials (from https://github.com/settings/developers).
	GitHubClientID     string `json:"github_client_id"`
	GitHubClientSecret string `json:"github_client_secret"`

	// GitLab OAuth app credentials (from https://gitlab.com/-/profile/applications).
	GitLabClientID     string `json:"gitlab_client_id"`
	GitLabClientSecret string `json:"gitlab_client_secret"`
	GitLabBaseURL      string `json:"gitlab_base_url"` // default "https://gitlab.com"
}

// CollectionConfig controls collection behavior.
type CollectionConfig struct {
	// BatchSize is the number of items to insert per database batch.
	BatchSize int `json:"batch_size"`

	// DaysUntilRecollect is how many days before re-collecting a repo.
	DaysUntilRecollect int `json:"days_until_recollect"`

	// Workers is the number of concurrent collection goroutines.
	Workers int `json:"workers"`

	// RepoCloneDir is the directory where repos are cloned for facade/commit
	// analysis. Can be terabytes for large instances. Defaults to $HOME/aveloxis-repos.
	RepoCloneDir string `json:"repo_clone_dir"`

	// ForceFullCollection when true makes every collection pass a full collection
	// (since=zero) regardless of when the repo was last collected. Use this to
	// re-collect all data after a bug fix (e.g., fixing contributor resolution).
	// Set to false after the full pass completes.
	ForceFullCollection bool `json:"force_full"`

	// MatviewRebuildDay is the day of the week to rebuild materialized views.
	// Valid values: "monday" through "sunday", or "disabled" to never auto-rebuild.
	// Default: "saturday". Views are rebuilt once per week on this day.
	MatviewRebuildDay string `json:"matview_rebuild_day"`

	// MatviewRebuildOnStartup controls whether materialized views are created/refreshed
	// during schema migration (startup). For large databases this can take minutes.
	// Default: false — views are created on first migrate but not refreshed on every startup.
	MatviewRebuildOnStartup bool `json:"matview_rebuild_on_startup"`

	// PRChildMode selects between the REST per-PR child waterfall
	// ("rest", default) and the batched GraphQL fetcher ("graphql").
	// When "graphql", the staged collector, open-item refresh, and gap
	// filler all use platform.Client.FetchPRBatch — one query for up
	// to 25 PRs and all their children. GitLab's FetchPRBatch falls
	// back to REST composition because GitLab's GraphQL API is weaker
	// on merge_request fields; parity is preserved at the column level.
	//
	// Default "rest" so existing deployments pick up v0.18.1 without a
	// behavior change until operators explicitly opt in.
	PRChildMode string `json:"pr_child_mode"`

	// ListingMode selects between two separate REST iterators for
	// issues and PRs ("rest", default) and the unified GraphQL
	// listing ("graphql") added in phase 2 of the REST→GraphQL
	// refactor. When "graphql", the staged collector calls
	// platform.Client.ListIssuesAndPRs once per repo instead of
	// iterating ListIssues and ListPullRequests separately. On GitHub
	// this is a pair of paginated GraphQL queries; on GitLab it
	// composes the existing REST iterators (GitLab's GraphQL MR
	// surface is too limited to use directly). Column parity is
	// preserved in both modes.
	//
	// Default "rest" so existing deployments pick up v0.18.2 without
	// a behavior change until operators explicitly opt in.
	ListingMode string `json:"listing_mode"`
}

// Load reads configuration from a JSON file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	cfg := DefaultConfig()
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}
	return cfg, nil
}

// SlogLevel returns the slog.Level corresponding to the LogLevel string.
func (c *Config) SlogLevel() slog.Level {
	switch strings.ToLower(c.LogLevel) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// MatviewRebuildWeekday returns the time.Weekday for the configured matview
// rebuild day, or -1 if disabled.
func (c *CollectionConfig) MatviewRebuildWeekday() int {
	switch strings.ToLower(c.MatviewRebuildDay) {
	case "sunday":
		return int(time.Sunday)
	case "monday":
		return int(time.Monday)
	case "tuesday":
		return int(time.Tuesday)
	case "wednesday":
		return int(time.Wednesday)
	case "thursday":
		return int(time.Thursday)
	case "friday":
		return int(time.Friday)
	case "saturday":
		return int(time.Saturday)
	case "disabled", "none", "off":
		return -1
	default:
		return int(time.Saturday) // default
	}
}

// DefaultConfig returns configuration with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Database: DatabaseConfig{
			Host:    "localhost",
			Port:    5432,
			User:    "augur",
			DBName:  "augur",
			SSLMode: "prefer",
		},
		GitHub: PlatformConfig{
			BaseURL: "https://api.github.com",
		},
		GitLab: PlatformConfig{
			BaseURL: "https://gitlab.com/api/v4",
		},
		Web: WebConfig{
			Addr:          ":8082",
			GitLabBaseURL: "https://gitlab.com",
		},
		Collection: CollectionConfig{
			BatchSize:               1000,
			DaysUntilRecollect:      1,
			Workers:                 12,
			RepoCloneDir:            defaultCloneDir(),
			MatviewRebuildDay:       "saturday",
			MatviewRebuildOnStartup: false,
			PRChildMode:             "rest",
			ListingMode:             "rest",
		},
		LogLevel: "info",
	}
}
