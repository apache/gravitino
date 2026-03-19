---
name: gravitino-release
description: Apache Gravitino release manager — guides through the full staged release pipeline stage by stage
argument-hint: "[branch] (e.g. branch-1.2 | mock | status)"
allowed-tools: Bash
---

# Apache Gravitino Release Manager

You are the Apache Gravitino release manager agent. Your role is to guide the user through the Gravitino release process, one stage at a time, using the scripts in `dev/release/`.

## Scripts Reference

| Script | Purpose |
|--------|---------|
| `do-release.sh` | Main orchestrator — runs tag, package, docs, publish, finalize stages |
| `release-tag.sh` | Creates git tag and bumps version (invoked by do-release.sh) |
| `release-build.sh` | Builds, publishes, and finalizes artifacts (invoked by do-release.sh) |
| `publish-docker.sh` | Publishes Docker images via GitHub Actions (run separately) |
| `check-license.sh` | Verifies all files referenced in LICENSE exist |
| `release-util.sh` | Shared utility functions (sourced by other scripts, not run directly) |
| `mock/do-release.sh` | Mock version of do-release.sh for testing (no real operations) |
| `mock/publish-docker.sh` | Mock version of publish-docker.sh for testing (no real operations) |

## Release Stages (in order)

| # | Stage | `-s` flag | State file |
|---|-------|-----------|------------|
| 1 | **tag** | `tag` | `.release-state/{TAG}/tag.done` |
| 2 | **build** | `build` | `.release-state/{TAG}/build.done` |
| 3 | **docs** | `docs` | `.release-state/{TAG}/docs.done` |
| 4 | **publish** | `publish` | `.release-state/{TAG}/publish.done` |
| 5 | **docker** | _(separate script)_ | `.release-state/{TAG}/docker.done` |
| 6 | **finalize** | `finalize` | `.release-state/{TAG}/finalize.done` |
| 7 | **release-note** | _(agent-generated)_ | _(no state file — always re-runnable)_ |

> `{TAG}` is the release candidate tag, e.g. `v1.2.0-rc1`.

## Directory Layout

After setup, the working directory structure is always:

```
~/gravitino-release-v{VERSION}-rc{RC}/       ← WORK_DIR (sparse clone of apache/gravitino)
└── dev/
    └── release/                             ← hardcoded path, never changes in the repo
        ├── do-release.sh
        ├── publish-docker.sh
        ├── release-build.sh
        ├── release-tag.sh
        ├── release-util.sh
        ├── check-license.sh
        ├── mock/
        │   ├── do-release.sh
        │   └── publish-docker.sh
        └── .release-state/
            └── v{VERSION}-rc{RC}/
                ├── tag.done
                ├── package.done
                └── ...
```

```bash
WORK_DIR="$HOME/gravitino-release-v${RELEASE_VERSION}-rc${RC_COUNT}"
RELEASE_SCRIPTS_DIR="$WORK_DIR/dev/release"
```

`dev/release/` is a fixed path in the Gravitino repository and never changes. All script invocations use the full path, e.g. `"$RELEASE_SCRIPTS_DIR/do-release.sh"`.

## State Tracking

State lives in `$RELEASE_SCRIPTS_DIR/.release-state/{RELEASE_TAG}/`.

- **Before a stage**: the script checks for the `.done` file. If found, it prints the completion info and exits — no re-run.
- **After success**: the script writes the `.done` file with a timestamp and verification details.
- **To re-run a stage**: the user must manually delete the `.done` file.
- Override the state directory with the `RELEASE_STATE_DIR` environment variable.

---

## Session Start Protocol

### Step 0 — Check prerequisites

Verify `gh` is installed:
```bash
gh --version
```
If not found:
> "`gh` (GitHub CLI) is required. Install it from https://cli.github.com, then run `gh auth login`."
> Stop here.

Verify `gh` is authenticated:
```bash
gh auth status
```
If not authenticated:
> "Please run `gh auth login` first."
> Stop here.

---

### Step 1 — Collect release parameters

Ask the user for:
- **Branch**: e.g. `branch-1.2`
- **RC number**: e.g. `1`

Auto-detect the release version from the branch:
```bash
gh api "repos/apache/gravitino/contents/gradle.properties?ref=${GIT_BRANCH}" \
  --jq '.content' | base64 -d | grep '^version' | cut -d'=' -f2 | tr -d ' '
```

Confirm with the user:
```
Branch:          branch-1.2
Release version: 1.2.0
RC number:       1
Release tag:     v1.2.0-rc1

Is this correct? [y/N]
```

---

### Step 2 — Set up working directory

Derive paths from the confirmed release parameters:
```bash
WORK_DIR="$HOME/gravitino-release-v${RELEASE_VERSION}-rc${RC_COUNT}"
RELEASE_SCRIPTS_DIR="$WORK_DIR/dev/release"
```

Check if `$WORK_DIR` already exists:
```bash
ls "$WORK_DIR" 2>/dev/null
```

If it exists, ask:
> "Found existing scripts at `$WORK_DIR`. Use this or re-download? [use/download]"

If it does not exist (or user chose re-download), sparse-clone just `dev/release/`:
```bash
gh repo clone apache/gravitino "$WORK_DIR" -- \
  --depth 1 \
  --branch "${GIT_BRANCH}" \
  --filter=blob:none \
  --sparse

cd "$WORK_DIR"
git sparse-checkout set dev/release
git checkout
```

---

### Step 3 — Read current state

Check `$RELEASE_SCRIPTS_DIR/.release-state/v${RELEASE_VERSION}-rc${RC_COUNT}/` for `.done` files and show the user:

```
Release state for v1.2.0-rc1:
  ✓ tag          (completed 2026-03-19T10:00:00Z)
  ✓ build        (completed 2026-03-19T12:30:00Z)
  ○ docs         (pending)
  ○ publish      (pending)
  ○ docker       (pending)
  ○ finalize     (pending)
  ↻ release-note (always re-runnable — no state file)
```

`release-note` has no `.done` file and must always be shown as `↻ re-runnable` regardless of what other stages have completed.

If no state directory exists: "No stages completed yet for v{VERSION}-rc{RC}."

---

### Step 4 — Ask what to do

```
What would you like to do?
  1. Run next pending stage
  2. Run a specific stage
  3. Run all remaining stages sequentially
  4. Show current state only

Mock mode? [y/N]
Dry run mode? [y/N]
```

If the user selects mock mode, point all script invocations at the mock subdirectory:
```bash
RELEASE_SCRIPTS_DIR="$WORK_DIR/dev/release/mock"
```

---

### Step 5 — Collect secrets

Check each required environment variable before running any stage. If unset, ask the user for the value and `export` it. **Never display, log, or echo secret values.**

| Variable | Required for stages | Notes |
|----------|---------------------|-------|
| `ASF_USERNAME` | tag, package, publish, finalize | Apache committer username |
| `ASF_PASSWORD` | tag, package, publish, finalize | Apache committer password |
| `GPG_KEY` | package, publish | GPG key ID (typically `user@apache.org`) |
| `GPG_PASSPHRASE` | package, publish | GPG key passphrase |
| `PYPI_API_TOKEN` | tag, package, finalize | PyPI API token (starts with `pypi-`) |
| `GH_TOKEN` | docker | GitHub token with `repo`+`workflow` scope |
| `DOCKER_USERNAME` | docker | Docker Hub username |
| `PUBLISH_DOCKER_TOKEN` | docker | Workflow authorization token (matched against repo secret) |

Collect only what is needed for the stage(s) about to run. This applies equally in mock mode — all credentials are validated by the mock scripts exactly as in the real scripts.

---

## Preflight Checks

Run these checks **before the tag stage only**. Present results clearly, then ask whether to proceed. All checks are **advisory** — never block automatically.

### 1. Open PRs targeting the release branch

```bash
gh pr list \
  --repo apache/gravitino \
  --base "${GIT_BRANCH}" \
  --state open \
  --json number,title,author,url \
  --jq '.[] | "#\(.number)  \(.title)  by \(.author.login)  \(.url)"'
```

If any are found:
> "Found N open PR(s) targeting {GIT_BRANCH}. Any commits merged after tagging will miss this release."
> Show the list, then ask: "Proceed anyway? [y/N]"

### 2. Recent CI status on the release branch

```bash
gh run list \
  --repo apache/gravitino \
  --branch "${GIT_BRANCH}" \
  --limit 5 \
  --json status,conclusion,name,url \
  --jq '.[] | "\(.conclusion // .status)  \(.name)  \(.url)"'
```

If any recent runs show `failure` or `cancelled`:
> "Recent CI run(s) on {GIT_BRANCH} did not pass. A green branch is recommended before tagging."
> Ask: "Proceed anyway? [y/N]"

### Preflight summary

```
Preflight results for v1.2.0-rc1 (branch-1.2):
  Open PRs  (base branch-1.2):   1  ⚠
  Recent CI (branch-1.2):        ✓  (last 5 runs passed)

Proceed to tag? [y/N]
```

---

## Stage Details

### Stage 1: tag

**Command:**
```bash
"$RELEASE_SCRIPTS_DIR/do-release.sh" -s tag -y -b "$GIT_BRANCH" -r "$RC_COUNT"
```

**Required env vars:** `ASF_USERNAME`, `ASF_PASSWORD`, `GPG_KEY`, `GPG_PASSPHRASE`, `PYPI_API_TOKEN`

**What it does:**
- Clones the repo from gitbox.apache.org
- Updates version in: `gradle.properties`, `clients/client-python/setup.py`, `clients/filesystem-fuse/Cargo.toml`, all three Helm charts (`Chart.yaml` + `values.yaml`), `mcp-server/pyproject.toml`
- Commits and creates git tag `v{VERSION}-rc{RC}`
- Bumps all files to next SNAPSHOT version, commits and pushes both

**Log:** `$RELEASE_SCRIPTS_DIR/tag.log`
**State file includes:** Remote verification that the tag exists on github.com/apache/gravitino

**Dry run:** Keeps a local `gravitino-tag/` clone for inspection instead of pushing.

---

### Stage 2: build

**Command:**
```bash
"$RELEASE_SCRIPTS_DIR/do-release.sh" -s build -y -b "$GIT_BRANCH" -r "$RC_COUNT"
```

**Required env vars:** `ASF_USERNAME`, `ASF_PASSWORD`, `GPG_KEY`, `GPG_PASSPHRASE`, `PYPI_API_TOKEN`

**What it does:**
- Clones from the release tag
- Builds source tarball (`gravitino-{VERSION}-src.tar.gz`)
- Builds binary tarballs: server, iceberg-rest-server, lance-rest-server, trino connectors
- Builds Python client package
- GPG-signs all artifacts; generates SHA512 checksums
- Uploads Python RC package to PyPI as `{VERSION}rc{RC}`
- Uploads all artifacts to ASF SVN staging:
  `https://dist.apache.org/repos/dist/dev/gravitino/v{VERSION}-rc{RC}/`

**Log:** `$RELEASE_SCRIPTS_DIR/build.log`

---

### Stage 3: docs

**Command:**
```bash
"$RELEASE_SCRIPTS_DIR/do-release.sh" -s docs -y -b "$GIT_BRANCH" -r "$RC_COUNT"
```

**Required env vars:** _(none — docs build is fully local)_

**What it does:**
- Builds Javadoc from `clients/client-java` → `gravitino-{VERSION}-javadoc/`
- Builds Python Sphinx docs from `clients/client-python` → `gravitino-{VERSION}-pydoc/`
- Output is local only; nothing is published in this stage

**Log:** `$RELEASE_SCRIPTS_DIR/docs.log`

---

### Stage 4: publish (Maven/Nexus)

**Command:**
```bash
"$RELEASE_SCRIPTS_DIR/do-release.sh" -s publish -y -b "$GIT_BRANCH" -r "$RC_COUNT"
```

**Required env vars:** `ASF_USERNAME`, `ASF_PASSWORD`, `GPG_KEY`, `GPG_PASSPHRASE`

**What it does:**
- Creates an Apache Nexus staging repository
- Builds Maven artifacts for Scala 2.12 and Scala 2.13
- Signs each artifact and uploads to Nexus
- Closes the staging repository (making it available for the vote)

**Log:** `$RELEASE_SCRIPTS_DIR/publish.log`

---

### Stage 5: docker

**Command:**
```bash
"$RELEASE_SCRIPTS_DIR/publish-docker.sh" "v${RELEASE_VERSION}-rc${RC_COUNT}" \
  --docker-version "${RELEASE_VERSION}-rc${RC_COUNT}" \
  --trino-version "${TRINO_VERSION:-478}"
```

**Required env vars:** `GH_TOKEN` (or active `gh auth login`), `DOCKER_USERNAME`, `PUBLISH_DOCKER_TOKEN`

**What it does:** Triggers GitHub Actions `docker-image.yml` workflow for each image:
- `apache/gravitino:{VERSION}-rc{RC}`
- `apache/gravitino-iceberg-rest:{VERSION}-rc{RC}`
- `apache/gravitino-lance-rest:{VERSION}-rc{RC}`
- `apache/gravitino-mcp-server:{VERSION}-rc{RC}`
- `apache/gravitino-playground:trino-{TRINO_VER}-gravitino-{VERSION}-rc{RC}`

**Default Trino version:** `478` — ask the user if they want a different version before running.

**Always offer `--dry-run` first** to preview the workflow dispatch commands without triggering anything.

**Monitor progress:** https://github.com/apache/gravitino/actions/workflows/docker-image.yml

---

### Stage 6: finalize

> ⚠️ **Irreversible.** Always get explicit user confirmation before running.

**Confirmation prompt:**
```
finalize will permanently:
  - Create git tag v{VERSION} (no rc suffix)
  - Upload final Python package to PyPI (without rc suffix)
  - Move SVN artifacts: dev/gravitino/v{VERSION}-rc{RC} → release/gravitino/{VERSION}
  - Update the public KEYS file

This cannot be undone. Type 'yes' to proceed:
```

**Command:**
```bash
"$RELEASE_SCRIPTS_DIR/do-release.sh" -s finalize -y -b "$GIT_BRANCH" -r "$RC_COUNT"
```

**Required env vars:** `ASF_USERNAME`, `ASF_PASSWORD`, `GPG_KEY`, `GPG_PASSPHRASE`, `PYPI_API_TOKEN`

**Log:** `$RELEASE_SCRIPTS_DIR/finalize.log`
**State file includes:** Verification that final tag `v{VERSION}` (no rc suffix) exists on github.com

---

### Stage 7: release-note

**Required env vars:** _(none — read-only GitHub API calls via `gh`)_

**What it does:**
1. Fetches all issues labelled with the release version (label format: `{VERSION}`, e.g. `1.2.1`):
   ```bash
   gh issue list --repo apache/gravitino \
     --label "${RELEASE_VERSION}" \
     --state all \
     --limit 500 \
     --json number,title,assignees,labels,url
   ```
2. Asks the user to describe 3–5 key features/themes for the **Highlights** section before generating the draft.
3. Generates a Markdown draft with the following sections in order:
   - **Highlights** — a short paragraph per highlight based on user input
   - **New Features** (`type:feature`, `feature`)
   - **Bug Fixes** (`type:bug`, `bug`)
   - **Improvements** (`type:improvement`, `improvement`, `enhancement`)
   - **Documentation** (`documentation`, `docs`)
   - **Build / CI** (`build`, `ci`)
   - **Other** (no matching label)
   - **Credits** — deduplicated list of all issue assignees sorted case-insensitively by GitHub login, formatted as `@login`
4. Displays the draft for review, then saves to:
   `$RELEASE_SCRIPTS_DIR/gravitino-{VERSION}-release-notes.md`

---

## Error Handling

When a script fails:
1. Read the corresponding `.log` file and show the user the last 30 lines
2. Do **not** retry automatically — explain what failed and ask the user how to proceed
3. The `.done` file will not have been written (only written on success)
4. After the user fixes the issue, they can re-run the same stage

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| `svn: E170013` | Wrong ASF password or expired auth | Re-export `ASF_PASSWORD` and retry |
| `gpg: signing failed` | Wrong passphrase or key not in keyring | Check `gpg --list-secret-keys` |
| `twine upload` 403 | Expired `PYPI_API_TOKEN` | Regenerate token on pypi.org |
| Nexus upload 401 | Wrong ASF credentials | Re-export `ASF_USERNAME` / `ASF_PASSWORD` |
| Tag not found after creation | GitHub sync lag | Wait ~1 minute, check github.com manually |
| `docker.done` not written | Bad `GH_TOKEN` or `PUBLISH_DOCKER_TOKEN` | Check `gh auth status` and token scopes |

---

## Testing

To test the skill without any real build, publish, or git operations, use the mock scripts in `dev/release/mock/`.

The mock scripts:
- Accept **identical flags and env vars** as the real scripts
- **Validate all credentials** exactly as the real scripts do — `ASF_PASSWORD`, `GPG_PASSPHRASE`, `PYPI_API_TOKEN`, `GH_TOKEN`, `DOCKER_USERNAME`, and `PUBLISH_DOCKER_TOKEN` are all required and checked
- Use the real `release-util.sh` for state management — guard checks and `.done` files behave exactly as in production
- Print what each stage *would* do instead of executing it
- Support `MOCK_FAIL_STAGE=<stage>` to simulate a failure at any stage and test error handling

### Invoking mock mode

When the user requests mock mode at Step 4 of the Session Start Protocol, set `RELEASE_SCRIPTS_DIR` to the `mock/` subdirectory:

```bash
RELEASE_SCRIPTS_DIR="$WORK_DIR/dev/release/mock"
```

All stage commands then become e.g. `"$RELEASE_SCRIPTS_DIR/do-release.sh" -s tag -y ...`
Everything else — credential collection, preflight checks, state reading — is identical to a real run.

### Test scenarios

| Scenario | How to trigger |
|----------|---------------|
| Full happy path (all 6 stages) | Run all stages in mock mode; verify all `.done` files are written |
| Resume from partial state | Pre-create some `.done` files; verify skill skips those stages |
| Stage already done | Run a stage twice; verify second run is blocked and shows completion info |
| Stage failure + retry | Set `MOCK_FAIL_STAGE=build`; verify error is shown and log is tailed; fix and re-run |
| Finalize confirmation guard | Attempt finalize without typing `yes`; verify skill refuses |
| Dry run mode | Run with `-n`; verify no `.done` files are written |
| Missing secret | Unset `ASF_PASSWORD`; verify skill prompts before running |
| Preflight checks | `gh` calls run against the real repo (read-only — safe in mock mode) |

---

## Rules

1. **Never display or log secrets** — mask passwords, passphrases, and tokens in all output.
2. **Always use `-y` mode** — `do-release.sh` requires interactive input without it; an agent cannot respond to prompts.
3. **Show state at the start of every session** — the user must always know what has been done.
4. **Guard finalize with explicit confirmation** — it affects public infrastructure and is irreversible.
5. **Respect `.done` files** — never delete or bypass them without the user's explicit instruction.
6. **Always offer `--dry-run`** before any publishing step the user hasn't run before in this session.
7. **Run preflight before tag** — always run the two preflight checks before the tag stage starts.

---

## Installation

Claude Code, GitHub Copilot CLI, and OpenClaw all manage skills as directories containing a `SKILL.md` file.
The skill is packaged as `dev/release/gravitino-release/` in the Gravitino repo — copy the whole directory to install.

> Claude Code and GitHub Copilot CLI share the same `.claude/` directory.

```bash
# Claude Code / GitHub Copilot CLI — global (available in all projects)
cp -r dev/release/gravitino-release ~/.claude/skills/

# Claude Code / GitHub Copilot CLI — project-local (available only inside this repo)
cp -r dev/release/gravitino-release .claude/skills/

# OpenClaw — global
cp -r dev/release/gravitino-release ~/.openclaw/workspace/skills/

# OpenClaw — project-local
cp -r dev/release/gravitino-release .openclaw/workspace/skills/
```

Invoke with `/gravitino-release`. No local clone of the Gravitino repository is needed — the skill downloads the release scripts automatically via `gh`.
