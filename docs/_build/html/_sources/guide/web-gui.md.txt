# Web GUI

Aveloxis includes a web-based GUI for managing repository groups with OAuth login. Users authenticate via GitHub or GitLab, create named groups, and add individual repositories or entire organizations/groups for collection.

## Prerequisites

Before starting the web GUI you need:

1. A working Aveloxis installation with `aveloxis migrate` already run.
2. At least one OAuth app registered with GitHub and/or GitLab.
3. The `web` section filled in within your `aveloxis.json` config file.

### Creating a GitHub OAuth App

1. Go to [https://github.com/settings/developers](https://github.com/settings/developers).
2. Click **New OAuth App**.
3. Fill in the fields:
   - **Application name**: anything you like (e.g., "Aveloxis").
   - **Homepage URL**: your Aveloxis web GUI URL (e.g., `http://localhost:8080`).
   - **Authorization callback URL**: `http://localhost:8080/auth/github/callback` (replace `http://localhost:8080` with your `web.base_url` if different).
4. Click **Register application**.
5. Copy the **Client ID** and generate a new **Client Secret**. Put both in your `aveloxis.json` under `web.github_client_id` and `web.github_client_secret`.

### Creating a GitLab OAuth App

1. Go to [https://gitlab.com/-/profile/applications](https://gitlab.com/-/profile/applications) (or the equivalent URL on your self-hosted GitLab instance).
2. Fill in the fields:
   - **Name**: anything you like (e.g., "Aveloxis").
   - **Redirect URI**: `http://localhost:8080/auth/gitlab/callback` (replace `http://localhost:8080` with your `web.base_url` if different).
   - **Scopes**: check `read_user`.
3. Click **Save application**.
4. Copy the **Application ID** and **Secret**. Put both in your `aveloxis.json` under `web.gitlab_client_id` and `web.gitlab_client_secret`.
5. If using a self-hosted GitLab instance, set `web.gitlab_base_url` to your instance URL (e.g., `https://gitlab.example.com`). The default is `https://gitlab.com`.

## Configuration

All web GUI settings live under the `web` key in `aveloxis.json`:

```json
{
  "web": {
    "addr": ":8080",
    "base_url": "http://localhost:8080",
    "session_secret": "change-me-to-a-random-string",
    "github_client_id": "Iv1.abc123...",
    "github_client_secret": "deadbeef...",
    "gitlab_client_id": "your-gitlab-app-id",
    "gitlab_client_secret": "your-gitlab-app-secret",
    "gitlab_base_url": "https://gitlab.com"
  }
}
```

| Field | Description | Default |
|---|---|---|
| `web.addr` | Listen address for the web server | `":8080"` |
| `web.base_url` | External URL used to construct OAuth callback URLs | `"http://localhost:8080"` |
| `web.session_secret` | Secret key for signing session cookies. Use a long random string. | (required) |
| `web.github_client_id` | Client ID from your GitHub OAuth app | `""` |
| `web.github_client_secret` | Client secret from your GitHub OAuth app | `""` |
| `web.gitlab_client_id` | Application ID from your GitLab OAuth app | `""` |
| `web.gitlab_client_secret` | Secret from your GitLab OAuth app | `""` |
| `web.gitlab_base_url` | Base URL for the GitLab instance (for self-hosted) | `"https://gitlab.com"` |

You only need to configure the providers you want to use. If you only use GitHub, you can leave the GitLab fields empty (and vice versa). The login page will only show buttons for configured providers.

## Starting the Web GUI

```bash
aveloxis web
```

The server starts on the address specified by `web.addr` (default `:8080`). Open `http://localhost:8080` in your browser.

## Login Flow

1. On the home page, click **Login with GitHub** or **Login with GitLab**.
2. You are redirected to the provider's authorization page. Approve access for the Aveloxis OAuth app.
3. The provider redirects back to Aveloxis with an authorization code.
4. Aveloxis exchanges the code for an access token and fetches your profile (login, email, avatar).
5. A session cookie is set in your browser. You are now logged in and redirected to the dashboard.

## Creating Groups

Groups are named collections of repositories. After logging in:

1. Click **New Group** on the dashboard.
2. Enter a group name and optional description.
3. Click **Create**. The empty group appears on your dashboard.

## Adding Individual Repos to a Group

1. Open a group from the dashboard.
2. Click **Add Repository**.
3. Paste the repository URL (e.g., `https://github.com/chaoss/augur` or `https://gitlab.com/fdroid/fdroidclient`).
4. Click **Add**. The repo is added to the group and automatically queued for collection.

Platform is auto-detected from the URL, the same as `aveloxis add-repo`.

## Adding an Entire GitHub Org or GitLab Group

1. Open a group from the dashboard.
2. Click **Add Organization**.
3. Paste the org URL (e.g., `https://github.com/chaoss` or `https://gitlab.com/gnome`).
4. Click **Add**. All current repositories in the org are added to the group and queued.

Behind the scenes, a row is inserted into the `user_org_requests` table to track the org for ongoing discovery.

## Navigation and Breadcrumbs

The web GUI uses breadcrumb navigation across the top of every page:

- **Dashboard**: Shows `Home` as the breadcrumb. Lists all your groups with repo counts.
- **Group detail page**: Shows `Home / {Group Name}`. Click `Home` to return to the dashboard.

## Comparing Repositories

Each group detail page (and the main dashboard) includes a **Compare Repositories** search widget. This uses the REST API (`aveloxis api`) to search across all repositories in the database — not just those in the current group. Select up to 5 repos, click **Compare**, and you are taken to the comparison page with your selection pre-populated.

The API must be running (`aveloxis api --addr :8383`) for the compare search to work.

## Searching and Pagination

Groups that track organizations can accumulate hundreds or thousands of repositories. The group detail page provides search and pagination to make large lists manageable.

### Search

At the top of the repository list, a search box lets you filter by name. The search is **case-insensitive** and matches against the repository name, owner, and full URL. For example, searching `augur` will match `chaoss/augur`, `chaoss/augur-community-reports`, etc.

Click **Clear** next to the search box to remove the filter and return to the full list. The result count updates to show how many repositories match your query.

### Pagination

Repositories are displayed **25 per page**. When a group has more than 25 repos, pagination controls appear at the bottom of the table:

- **First** / **Last** links to jump to the beginning or end.
- **Previous** / **Next** links to move one page at a time.
- A **sliding window of 5 page numbers** centered on the current page, so the controls stay compact even with hundreds of pages.
- The current page number is highlighted.

Search and pagination work together: if you search for `chaoss` and there are 40 matches, you see 25 on page 1 and 15 on page 2. The search query is preserved as you navigate between pages.

## How Org Tracking Works

When you add an org, Aveloxis does not just snapshot the current repo list -- it continuously monitors the org for new repos:

- A scheduler task runs **every 4 hours** and re-fetches the repository list for every org in `user_org_requests`.
- Any newly created repos that are not already in the database are added to the group and queued for collection automatically.
- Repos that are deleted or made private on the forge are handled by the existing dead repo sidelining logic during collection.

This means you can add `https://github.com/kubernetes` once and Aveloxis will automatically pick up new repos as the Kubernetes org creates them.

## How Repos Get Queued for Collection

Repos added through the web GUI (individually or via org tracking) are inserted into the same `aveloxis_ops.collection_queue` used by the CLI. They are collected by `aveloxis serve` in priority order, just like repos added via `aveloxis add-repo`.

You must have `aveloxis serve` running for collection to happen. The web GUI only manages the queue -- it does not collect data itself.

## Session Management

- Sessions are stored **in-memory** on the web server process.
- Each session expires after **24 hours** of inactivity.
- Restarting `aveloxis web` clears all sessions. Users will need to log in again.
- Sessions are tied to a secure, signed cookie. The signing key is `web.session_secret` from your config.

## Running Alongside `aveloxis serve`

The web GUI and the collection scheduler are separate processes. In a typical deployment you run both:

```bash
# Terminal 1: collection scheduler with monitoring dashboard
aveloxis serve --workers 4 --monitor :5555

# Terminal 2: web GUI for group management
aveloxis web
```

They share the same PostgreSQL database. The web GUI writes to the queue; the scheduler reads from it. There is no direct communication between the two processes.

You can run them on different hosts as long as both can reach the database.

## Security Considerations

- **OAuth tokens**: The access tokens obtained during login are used only to fetch the user's profile and are not stored persistently. They are held in the session for the duration of the login.
- **Session cookies**: Signed with `web.session_secret`. Use a strong, random secret in production. If the secret is compromised, an attacker could forge session cookies.
- **HTTPS**: In production, run `aveloxis web` behind a reverse proxy (nginx, Caddy, etc.) that terminates TLS. OAuth providers require HTTPS callback URLs for production apps (localhost is exempt during development).
- **Client secrets**: The `web.github_client_secret` and `web.gitlab_client_secret` values in `aveloxis.json` are sensitive. Protect the config file with appropriate file permissions (`chmod 600 aveloxis.json`).
- **No role-based access control**: Currently all authenticated users have the same permissions. Any logged-in user can create groups and add repos. If you need to restrict access, control who can reach the web GUI at the network level.
