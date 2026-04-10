# Installation

This guide covers installing Aveloxis and its dependencies from source.

---

## Requirements

Before installing Aveloxis, ensure you have the following:

| Dependency | Minimum Version | Purpose |
|---|---|---|
| **Go** | 1.23+ | Compiles and runs Aveloxis |
| **PostgreSQL** | 14+ | Stores all collected data and operational state |
| **git** | Any recent version | Used by the facade phase for bare clones and `git log` parsing |

You also need at least one **GitHub personal access token** (with `repo` or `read` scope) and/or a **GitLab personal access token** (with `read_api` scope) to collect data from those platforms.

### Verify prerequisites

```bash
go version        # Should print go1.23 or later
psql --version    # Should print 14.x or later
git --version     # Any recent version
```

---

## Install via `go install` (recommended)

This method places the `aveloxis` binary in your `$GOPATH/bin` directory (or `$HOME/go/bin` if `$GOPATH` is not set), making it available system-wide.

```bash
git clone https://github.com/augurlabs/aveloxis.git
cd aveloxis
go mod tidy
go install ./cmd/aveloxis
```

Verify the installation:

```bash
aveloxis version
```

```{tip}
If you see `aveloxis: command not found`, your Go bin directory is not on your PATH. See [PATH troubleshooting](#path-troubleshooting) below.
```

---

## Install via `go build` (local binary)

This method builds the binary into the repository directory. Use this if you prefer not to install globally.

```bash
git clone https://github.com/augurlabs/aveloxis.git
cd aveloxis
go mod tidy
go build -o bin/aveloxis ./cmd/aveloxis
```

Verify:

```bash
./bin/aveloxis version
```

```{note}
With this method, you must use `./bin/aveloxis` (or the full path) instead of `aveloxis` for all commands. All other documentation assumes you used `go install`.
```

---

## PATH troubleshooting

If `aveloxis version` prints `command not found` after `go install`, the Go bin directory is not in your `PATH`.

### Find the Go bin directory

```bash
echo "$(go env GOPATH)/bin"
```

This typically prints `$HOME/go/bin`.

### Add it to your PATH

Add the following line to your shell profile (`~/.bashrc`, `~/.zshrc`, or `~/.profile`):

```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

Then reload your shell:

```bash
source ~/.zshrc   # or ~/.bashrc
```

### Verify

```bash
which aveloxis
aveloxis version
```

---

## Installing optional tools

Aveloxis can perform per-file code complexity analysis using [scc](https://github.com/boyter/scc) (Sloc Cloc and Code). This is optional -- if `scc` is not installed, the code complexity phase is silently skipped.

To install `scc`:

```bash
aveloxis install-tools
```

This downloads and compiles `scc` using `go install`. It requires Go to be installed (which you already have if you built Aveloxis).

After installation, the analysis phase will automatically populate the `repo_labor` table with per-file metrics including:

- Programming language
- Total lines, code lines, comment lines, blank lines
- Cyclomatic complexity

---

## Verifying installation

Run the following to confirm everything is working:

```bash
# Check the binary
aveloxis version

# Check that scc is installed (optional)
which scc
scc --version
```

If you have a database ready, you can also verify database connectivity by running migrations:

```bash
aveloxis migrate
```

This creates all required schemas and tables. See [Configuration](configuration.md) for how to set up `aveloxis.json` with your database credentials before running this.

---

## Updating

To update to the latest version:

```bash
cd /path/to/aveloxis
git pull
go mod tidy
go install ./cmd/aveloxis    # or: go build -o bin/aveloxis ./cmd/aveloxis
```

Then restart any running `aveloxis serve` instances:

```bash
aveloxis stop
aveloxis serve --monitor :5555
```

---

## Next steps

- [Configuration](configuration.md) -- set up `aveloxis.json`
- [Quick Start](quickstart.md) -- collect your first repo in 5 steps
- [Augur Migration](augur-migration.md) -- migrate from an existing Augur installation
