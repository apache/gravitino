<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Contributing to Apache Gravitino

Welcome! This guide will help you get started contributing to Apache Gravitino, whether you're filing an issue, improving documentation, or submitting code.

Gravitino welcomes all kinds of contributions—including code (Java, Python), documentation, testing, design, and feedback. Your involvement helps strengthen the project.

Apache Gravitino is a metadata and data lake federation layer for AI and analytics. It graduated from the Apache Software Foundation's Incubator in June 2025 and is now a top level project maintained by a growing community.

Gravitino follows ASF-wide contribution and governance practices. Project-specific workflows are explained where they differ.

Please review the following guidelines for a smooth contribution process.

## 📚 Table of Contents

* [📁 Project Overview and Policies](#-project-overview-and-policies)
* [💬 Community and Communication](#-community-and-communication)
* [🔐 Code of Conduct](#-code-of-conduct)
* [📬 How Decisions Are Made](#-how-decisions-are-made)
* [🌱 Growing Your Involvement](#-growing-your-involvement)
* [🧪 Continuous Integration](#-continuous-integration)
* [🆘 Need Help?](#-need-help)
* [🚀 Getting Started](#-getting-started)
* [🛍️ Quick Start for First-Time Contributors](#-quick-start-for-first-time-contributors)
* [🙌 Making Contributions](#-making-contributions)
* [📝 Contributing to Design and Documentation](#-contributing-to-design-and-documentation)
* [🔍 Reviewing and Triaging Contributions](#-reviewing-and-triaging-contributions)
* [🔃 Creating Pull Requests](#-creating-pull-requests)
* [💻 Setting Up Development Environment](#-setting-up-development-environment)
* [🧰 Setting Up Your IDE (Optional)](#-setting-up-your-ide-optional)
* [💡 Tips for WSL Users](#-tips-for-wsl-users)
* [⚙️ Build Profiles and JDK Requirements](#-build-profiles-and-jdk-requirements)
* [🎨 Formatting Code with Spotless](#-formatting-code-with-spotless)
* [🗂️ File Structure Overview](#-file-structure-overview)
* [🧪 Writing and Running Tests](#-writing-and-running-tests)
* [🧱 Following Coding Standards](#-following-coding-standards)
* [📆 Managing Dependencies](#-managing-dependencies)
* [🛡️ Performing Compliance and Legal Checks](#-performing-compliance-and-legal-checks)
* [📄 License](#-license)
* [🔐 Reporting Security Issues](#-reporting-security-issues)
* [🙏 Acknowledging Contributors](#-acknowledging-contributors)
* [📘 Glossary](#-glossary)

## 📁 Project Overview and Policies

* **Overview**: See the [Gravitino website](https://gravitino.apache.org) and [README.md](README.md).
* **Governance**: See [GOVERNANCE.md](GOVERNANCE.md).
* **Roadmap**: See [ROADMAP.md](ROADMAP.md).

Apache Gravitino is a [Top-Level Project of the ASF](https://www.apache.org/foundation/).

## 💬 Community and Communication

Gravitino is a collaborative project. We encourage open communication and transparent decision-making. You can:

* Subscribe to the developer mailing list: `dev@gravitino.apache.org`
  * [View archives](https://lists.apache.org/list.html?dev@gravitino.apache.org)
* Discuss issues via [GitHub Issues](https://github.com/apache/gravitino/issues)
* Join discussions via ASF Slack (invite link: [https://s.apache.org/slack-invite](https://s.apache.org/slack-invite))

Please follow the [ASF Code of Conduct](https://www.apache.org/foundation/policies/conduct.html) in all interactions.

## 🔐 Code of Conduct

We are committed to fostering a welcoming and inclusive community. Please review and adhere to our [Code of Conduct](https://github.com/apache/gravitino/blob/main/CODE_OF_CONDUCT.md) in all project spaces.

## 📬 How Decisions Are Made

Gravitino uses [Apache’s consensus-based decision-making](https://www.apache.org/foundation/glossary.html#ConsensusApproval). Most decisions happen on the `dev@` mailing list or via GitHub PR discussions.

## 🌱 Growing Your Involvement

Contributors who participate actively and constructively may be invited to become committers or join the PMC. Merit is earned through consistent contributions and community engagement.

## 🧪 Continuous Integration

Pull request tests run with GitHub Actions in your fork, not in `apache/gravitino`:

1. Enable GitHub Actions in your fork (the **Actions** tab of your fork).
2. Push your branch to your fork. **Fork CI** resolves one upstream base commit and every suite tests the same merge of that base and your pushed commit.
3. Open the pull request. Its **Fork CI** check verifies the suite jobs and the exact tested head, base branch and base SHA. A pre-PR run is accepted only if its inferred base matches the PR. After retargeting a PR, push again or ask a committer for a trusted run. The separate **ASF Allowlist Check** scans the PR's workflow YAML using reviewed upstream code.
4. Keep your branch up to date before merging. Strict branch protection requires an up-to-date branch. If the base changes, update your branch and run CI again. The **Fork CI** check can stay green after the base branch moves on; strict branch protection, not the check, blocks the merge until the branch is updated.

### Closing and reopening a PR

For PRs using Fork CI, closing and reopening the PR does **not** restart CI:

| Action | Fork CI behavior | PR status |
| --- | --- | --- |
| Push another commit to the same fork branch | Cancels the previous run and starts a run for the new commit | Tracks the new commit and run |
| Close the PR | Existing fork CI continues; it is not automatically canceled | Status synchronization stops while the PR is closed |
| Reopen the PR without another push | Does not start a new run or rerun an old one; a running fork CI continues | Refreshes the latest fork run's result |

To stop CI after closing a PR, cancel the run in your fork's **Actions** tab. Reopening a PR does not clear a failed or canceled result: it remains unsuccessful until CI is run again and passes.

### Starting or rerunning Fork CI

After reopening a PR, use either of these methods to run CI again:

* **Rerun the current commit:** In your fork, open **Actions → Fork CI** and select the run for your PR branch's current commit. Once the run has finished, choose **Re-run all jobs**. For failed runs, **Re-run jobs → Re-run failed jobs** can retry failed and canceled jobs without repeating successful jobs. See [GitHub's rerun instructions](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs).
* **Push a new commit:** Push another commit to the PR's source branch in your fork. This starts a new Fork CI run. If you only need to trigger CI, you can use an empty commit instead of changing a file. From your PR branch, replace `FORK_REMOTE` and `PR_BRANCH` with your fork remote and PR source branch:

  ```shell
  git commit --allow-empty -m "Trigger Fork CI"
  git push FORK_REMOTE HEAD:PR_BRANCH
  ```

If no fork run starts, enable Actions in your fork, update your branch from the latest base branch, and push again. A branch created before Fork CI was introduced does not contain `.github/workflows/fork-ci.yml`, so pushing it runs no CI; the check then asks you to merge or rebase onto the latest base branch. If an old run is no longer available for rerun, push a new commit instead. Fork CI cancels the rest of a run when a job fails, so wait for cancellation to finish before rerunning jobs. Please fix failures before requesting a review.

Reopening the PR or dispatching **Update Fork CI Status** only refreshes results; neither starts fork CI. If the check requests trusted verification for CI-definition changes, a committer must dispatch the trusted upstream **Fork CI** run described below; rerunning the fork does not satisfy that requirement.

### CI status synchronization

Status synchronization closes unfinished checks for confirmed canceled runs among the fork branch's ten most recent runs.

Fork runs cannot notify `apache/gravitino` when they finish, so a scheduled sweep polls recently updated PRs every two minutes for 11 minutes; GitHub starts these sweeps every 10-40 minutes. PR events are processed in order in a separate queue of up to 100 runs, so they never wait behind a sweep. Before writing the commit status, each publisher re-reads the check and publishes what it records, and the next pass repairs any interleaving. GitHub cancels excess runs when the queue is full; later sweeps recover Fork CI updates, and committers can manually dispatch either publisher for a specific PR.

The publishers report **Fork CI** and **ASF Allowlist Check** as commit statuses with direct links to the corresponding runs. Additional check runs retain diagnostic metadata; GitHub can omit those checks from the PR's latest-check view after another signal run. Queued and running CI both have a pending status, with the description distinguishing whether jobs have started. This also applies to PRs that edit CI definitions: their check explains that trusted verification will be needed, while continuing to show execution progress. Failed tests report failure; cancellation reports error. A successful fork run with changed CI definitions stays pending as **Fork CI passed; trusted verification required** until a trusted run verifies the PR. The job that stops a run on the first failure only observes the other jobs: its own errors never fail the run.

Known limitations of Fork CI:

* New/edited PR notifications wake a default-branch publisher. Fork run completions and reruns, including reruns of successful runs, are polled on a five-minute schedule, subject to GitHub scheduling delays and a bounded API budget. All open PRs, including settled successes, remain watched. The starting PR rotates on each scheduled interval; large queues can take longer. Committers can force-refresh a particular PR using **Update Fork CI Status** with its PR number.
* A pull request last pushed before this migration needs an update and another push. Old runs without the tested-context metadata cannot satisfy the new check.
* Coverage comments remain available for release branches. For PRs to `main`, inspect the fork run's `build / coverage` log and `coverage-report` artifact.
* Release branches (`branch-*`) keep their own upstream CI until this change is backported.
* CI workflow, action, helper-script or `.asf.yaml` changes cannot approve themselves. After reviewing those changes, a committer runs **Fork CI** in `apache/gravitino`, from the current default branch, and supplies the PR number. This explicit exception uses upstream runner resources, reviewed default-branch workflow definitions, a read-only token and no repository secrets; it tests the exact contributor head against the current base. The run resolves the branch's current SHA even when the PR retains an older base snapshot. A later contributor push requires a new run; an existing trusted success remains valid while its tested base stays on the same base branch, with branch protection controlling freshness.
* The independent allowlist scanner downloads bounded YAML data outside its trusted checkout and never executes PR code. It reports the **ASF Allowlist Check** on every PR, including PRs that do not change `.github/`. A committer can re-run **Trusted ASF Allowlist** with the PR number.

**Fork CI** and **ASF Allowlist Check** are not required status checks, so a trusted run stuck in the shared runner queue does not block an urgent merge. Branch protection still requires an up-to-date branch and an approving review. Release-branch protection is unchanged.

The trusted publishers use `workflow_run` and `schedule`, rather than `pull_request_target`. `actions/*`, `github/*` and `apache/*` actions are exempt from ASF's external-action SHA requirement; external third-party actions still require an approved SHA.

## 🆘 Need Help?

If you're stuck, ask on the `dev@` mailing list or open a GitHub Discussion. We're here to help.

## 🚀 Getting Started

### Fork the Repository

Click the "Fork" button on GitHub or use:

```bash
git clone https://github.com/apache/gravitino.git
cd gravitino
```

Check out the [ASF New Contributor Guide](https://www.apache.org/dev/new-committers-guide.html) for an overview of how ASF projects work.

## 🛍️ Quick Start for First-Time Contributors

1. Fork and clone the repository
2. Build the project using [how to build](/docs/how-to-build.md)
3. Pick an issue or start a discussion
4. Submit a pull request and collaborate with reviewers

## 🙌 Making Contributions

We welcome all types of contributions:

* **Code** – bug fixes, new features, refactoring, integrations
  * If you're adding or updating a connector, catalog, or integration, please discuss it on the dev@ mailing list first.
* **Documentation** – tutorials, guides, references
* **Review** – triage and review issues and PRs
* **Community** – answer questions, join discussions

Look for [good first issues](https://github.com/apache/gravitino/labels/good%20first%20issue) to get started. Discuss plans on GitHub or the dev@ mailing list. Small pull requests are easiest to review.

## 📝 Contributing to Design and Documentation

Design and documentation are essential to Gravitino’s usability and growth.

You can contribute by:

* Improving documentation in `docs/`
* Clarifying APIs and references
* Reviewing documents for accuracy
* Submitting mockups or usability suggestions

## 🔍 Reviewing and Triaging Contributions

Reviewing and triaging helps maintain the project. You can:

* Reproduce bugs and confirm issues
* Add labels to categorize
* Suggest improvements on PRs
* Review code and give feedback
* Review changes under `.github/` and `.asf.yaml` carefully, then dispatch a trusted upstream **Fork CI** run for those changes. Check the displayed tested base SHA and all caller suite results before merging.
* Merge only after **Fork CI** and **ASF Allowlist Check** pass. They are not enforced by branch protection; if you must merge without them, for example while a trusted run is queued, comment on the PR with what you verified instead.
* Please add Copilot before you review. That is recommended, not required. Do not wait on Copilot if you are ready to review.

## 🔃 Creating Pull Requests

* Use feature branches
* Write clear commit messages and PR descriptions
* Link to issues (e.g., `Fixes #123`)
* Respond to reviewer feedback
* Please add Copilot on the PR. Automatic review runs once when the PR is marked ready; later pushes do not re-review, so add Copilot again after you push. Treat Copilot comments as hints: implement the suggestion, or reply with why you are not taking it. Human reviewers remain responsible for the merge decision. Project-specific Copilot guidance lives in [`.github/copilot-instructions.md`](.github/copilot-instructions.md).

## 💻 Setting Up Development Environment

Follow [how to build](/docs/how-to-build.md) or use [Apache DockerHub](https://hub.docker.com/u/apache). Start Gravitino with:

```bash
bin/gravitino.sh start
bin/gravitino.sh stop
```

You can also build manually with:

```bash
./gradlew build
```

## 🧰 Setting Up Your IDE (Optional)

Import Gravitino into IntelliJ IDEA or Eclipse as a Gradle project.

## 💡 Tips for WSL Users

If using WSL on Windows, install Java and dev tools inside WSL. Access files via `/mnt/c/...` paths.

## ⚙️ Build Profiles and JDK Requirements

Gravitino uses Gradle properties for Scala:

* `-Pscala-2.12` for Scala 2.12
* `-Pscala-2.13` for Scala 2.13 (default)

Gravitino supports JDK 17.

## 🎨 Formatting Code with Spotless

We use [Spotless](https://github.com/diffplug/spotless) for code formatting. Run:

```bash
./gradlew spotlessApply
```

## 🗂️ File Structure Overview

Key directories in the Gravitino repo:

* `api/`, `clients/`, `core/`, `server/` – Main components
* `docs/`, `rfc/` – Documentation and design
* `conf/`, `bin/` – Configs and scripts
* `catalogs/`, `spark-connector/`, `trino-connector/` – Integrations

## 🧪 Writing and Running Tests

Add or update tests in your PR. Test open PRs locally to help maintainers.

## 🧭 Following Coding Standards

* Follow Java and Python idioms
* Include useful comments
* Keep methods and classes focused
* Format with Spotless

## 📦 Managing Dependencies

* Avoid unnecessary dependencies
* Prefer well-maintained libraries
* Discuss major additions on dev@

## 🛡️ Performing Compliance and Legal Checks

Before contributing:

* Review [ASF License and IP Guidelines](https://www.apache.org/legal/)
* Ensure all code is original or properly licensed and compatible with the Apache License

## 📄 License

Contributions are under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).

If contributing on behalf of an employer or regularly, you may need to submit an [ICLA](https://www.apache.org/licenses/#contributor-license-agreements). See [ASF Contributor FAQs](https://www.apache.org/dev/new-committers-guide.html#cla).

Official repo: [https://github.com/apache/gravitino](https://github.com/apache/gravitino)

## 🔐 Reporting Security Issues

To report a vulnerability, follow the [SECURITY.md](SECURITY.md) instructions. Responsible disclosure is appreciated and handled per ASF guidelines.

## 🙏 Acknowledging Contributors

Contributors are recognized in release notes. All types of contributions are valued.

See the full list of contributors on GitHub: [https://github.com/apache/gravitino/graphs/contributors](https://github.com/apache/gravitino/graphs/contributors)

## 📘 Glossary

* **ASF**: Apache Software Foundation
* **ICLA**: Individual Contributor License Agreement
* **PMC**: Project Management Committee
* **RAT**: Release Audit Tool
* **Spotless**: Code formatter
* **CI**: Continuous Integration
* **PR**: Pull Request
* **WSL**: Windows Subsystem for Linux
* **TLP**: Top-Level Project
* **dev@ list**: Primary development mailing list
* **AI**: Artificial Intelligence
* **IDE**: Integrated Development Environment
