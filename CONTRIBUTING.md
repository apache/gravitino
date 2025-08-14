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

All PRs are automatically tested using GitHub Actions. Please check test results and fix failures before requesting a review.

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

## 🔃 Creating Pull Requests

* Use feature branches
* Write clear commit messages and PR descriptions
* Link to issues (e.g., `Fixes #123`)
* Respond to reviewer feedback

## 💻 Setting Up Development Environment

Follow [how to build](/docs/how-to-build.md) or use [Apache DockerHub](https://hub.docker.com/u/apache). Start Gravitino with:

```bash
bin/gravitino.sh start
bin/gravitino.sh stop
```

You can also build manually with:

```bash
./mvnw clean install
```

## 🧰 Setting Up Your IDE (Optional)

Import Gravitino into IntelliJ IDEA or Eclipse as a Gradle project.

## 💡 Tips for WSL Users

If using WSL on Windows, install Java and dev tools inside WSL. Access files via `/mnt/c/...` paths.

## ⚙️ Build Profiles and JDK Requirements

Gravitino uses Maven profiles for Scala:

* `-Pscala-2.12` for Scala 2.12
* `-Pscala-2.13` for Scala 2.13 (default)

Gravitino supports JDK 8, 11 and 17.

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
* Ensure all code is original or properly licensed and compatable with the Apache License

## 📄 License

Contributions are under the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).

If contributing on behalf of an employer or regularly, you may need to submit an [ICLA](https://www.apache.org/licenses/icla.html). See [ASF Contributor FAQs](https://www.apache.org/dev/new-committers-guide.html#cla).

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
