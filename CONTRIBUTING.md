<!--
  Copyright 2023 Datastrato Pvt Ltd.
  This software is licensed under the Apache License version 2.
-->

# Contributing to Gravitino

Thank you for your interest in contributing to Gravitino! You are welcome to contribute in any way you can to enhance the project. Gravitino appreciates your assistance in making it better, whether through code contributions, documentation, tests, best practices, graphic design, or any other means that have a positive impact.

Before you get started, please read and follow these guidelines to ensure a smooth and productive collaboration.

## Table of contents

- [Getting Started](#getting-started)
  - [Fork the Repository](#fork-the-repository)
- [Contribution Guidelines](#contribution-guidelines)
  - [Code of Conduct](#code-of-conduct)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
  - [Good First Issues](#good-first-issues)
  - [Working on Issues](#working-on-issues)
  - [Creating Pull Requests](#creating-pull-requests)
- [Development Setup](#development-setup)
- [Testing](#testing)
- [Coding Standards](#coding-standards)
- [Community and Communication](#community-and-communication)
- [License](#license)

## Getting started

### Fork the repository

Either click the "Fork" button at the top right of the repository's page on GitHub OR create a fork on your local machine using `git clone`.

```bash
git clone https://github.com/datastrato/gravitino.git
cd gravitino
```

Now you are ready to start making contributions.

## Contribution guidelines

### Code of conduct

Please read and follow the [Code of Conduct](CODE_OF_CONDUCT.md). Gravitino provides a welcoming and inclusive environment for all contributors.

### Reporting bugs

If you find a bug in Gravitino, please open an issue on GitHub. Be sure to include as much detail as possible, such as a clear description, steps to reproduce, and your environment. Please follow the template provided.

### Suggesting enhancements

If you have ideas for enhancements or new features, feel free to create an issue to discuss them. Gravitino welcomes suggestions and provides prompt feedback on their feasibility and relevance.

### Good first issues

If you are new to open source or can't find something to work on check out the [Good First Issues list](https://github.com/datastrato/gravitino/contribute).

### Working on issues

Check out the list of open issues and find one that interests you. You can also comment on an issue to indicate that you're working on it. Please keep the issue updated with your progress.

## Creating pull requests

Create a new branch from ``main`` for your changes:

```bash
git checkout -b your-branch-name
```

Make your changes and commit them. Be sure to write clear and concise commit messages.

```bash
git commit -m "Your commit message"
```

Push your changes to your fork on GitHub:

```bash
git push your-branch-name
```

After you have pushed your changes, create a pull request (PR) in the Gravitino repository. Be sure to provide a detailed description of your changes, reference any related issues and please follow the template provided. Gravitino's maintainers evaluate pull requests, offer feedback, and assist in merging project changes.

## Development setup

Once you have cloned the [GitHub repository](https://github.com/datastrato/gravitino), see [how to build](/docs/how-to-build.md) for instructions on how to build, or you can use the provided docker images at [Datastrato's DockerHub repository](https://hub.docker.com/u/datastrato).

To stop and start a local Gravitino server via ``bin/gravitino.sh start`` and ``bin/gravitino.sh stop`` in a Gravitino distribution, see [how to build](/docs/how-to-build.md) for more instructions.

## Testing

The CI infrastructure runs unit and integration tests on each pull request, please make sure these tests pass before making a pull request.

The unit tests run on every build and integration tests run as needed. See [how to test](docs/how-to-test.md) for more information.

When adding new code or fixing a bug be sure to add unit tests to provide coverage.

## Coding standards

Spotless checks code formatting. If your code isn't correctly formatted, the build fails. To correctly format your code please use Spotless.

```bash
./gradlew spotlessApply
```

All files must have a license header and the build fails if any files are missing license headers. If you are adding third-party code be sure to understand how to add the third-party license to Gravitino LICENSE and NOTICE files.

For any bugs or new code please add unit tests to provide coverage of the code. The project may not accept code without unit tests.

All text files should use macOS/unix style line endings (LF) not windows style line endings (CRLF).

## Community and communication

Join the [community discourse group](https://gravitino.discourse.group) to discuss ideas and seek help. You are also encouraged to use GitHub discussions and follow Datastrato on social media to stay updated on project news.

## License

When contributing to this project, you agree that your contributions use the Apache License version 2. Please ensure you have permission to do this if required by your employer.

Thank you for your contributions to Gravitino! The project appreciates your help in making it a success.
