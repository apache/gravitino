<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# gravitino-release skill

A Claude Code skill that guides you through the full Apache Gravitino release pipeline, stage by stage.

## What it does

The skill orchestrates the 8-stage release process:

| # | Stage | Description |
|---|-------|-------------|
| 1 | **tag** | Creates git tag and bumps version |
| 2 | **build** | Builds and uploads artifacts to ASF SVN staging |
| 3 | **docs** | Builds Javadoc and Python Sphinx docs |
| 4 | **publish** | Publishes Maven artifacts to Apache Nexus |
| 5 | **docker** | Publishes RC Docker images via GitHub Actions |
| 6 | **finalize** | Promotes artifacts to release (irreversible) |
| 7 | **docker-final** | Publishes final Docker images |
| 8 | **release-note** | Generates a Markdown release note draft |

Long-running stages (build, docs, publish, finalize) run in the background so you can keep chatting while they complete.

## Installation

Copy the skill directory to Claude Code's skills folder:

```bash
# Global — available in all projects
cp -r agent-skills/gravitino-release ~/.claude/skills/

# Project-local — available only inside this repo
cp -r agent-skills/gravitino-release .claude/skills/
```

## Prerequisites

- **`gh` CLI** installed and authenticated (`gh auth login`)
- Apache committer credentials (`ASF_USERNAME`, `ASF_PASSWORD`)
- GPG key set up for signing (`GPG_KEY`, `GPG_PASSPHRASE`)
- PyPI API token (`PYPI_API_TOKEN`) for Python package publishing
- Docker Hub credentials and GitHub token for Docker stages

The skill will prompt you for any missing credentials before each stage runs.

## Usage

Invoke the skill from Claude Code:

```
/gravitino-release branch-1.2
```

At startup the skill will:
1. Verify `gh` is installed and authenticated
2. Ask for the release branch and RC number, then confirm the derived version and tag
3. Sparse-clone `dev/release/` from the release branch (no full local clone needed)
4. Show the current release state (which stages are done)
5. Ask what you want to do next

### Mock mode

Test the full pipeline without making any real changes:

```
/gravitino-release mock
```

Mock scripts validate all credentials and simulate each stage, writing real `.done` state files so you can test resume, retry, and failure scenarios.

Set `MOCK_STAGE_DELAY=<seconds>` to simulate long-running stages (e.g. `export MOCK_STAGE_DELAY=180`).

### Checking status mid-run

While a long stage is running in the background, just ask:

> "How's the build going?"

Claude will tail the log and report back. You can also use `/loop` to poll automatically:

```
/loop 10m check stage build status
```

The release scripts themselves live in `dev/release/` inside a fresh clone of `apache/gravitino` — they are downloaded automatically when you start the skill.
