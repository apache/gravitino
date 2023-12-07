<!--
  Copyright 2023 DATASTRATO Pvt Ltd.
  This software is licensed under the Apache License version 2.
-->

# Contributing to Gravitino

Thank you for your interest in contributing to gravitino! We welcome all forms of contribution from the community, and we appreciate your help in making this project better. Code contributions are welcome, but we also accept documentation, tests, best practices, graphic design, or anything else that can help the project.

Before you get started, please read and follow these guidelines to ensure a smooth and productive collaboration.

## Table of Contents

- [Getting Started](#getting-started)
  - [Fork the Repository](#fork-the-repository)
- [Contribution Guidelines](#contribution-guidelines)
  - [Code of Conduct](#code-of-conduct)
  - [Reporting Bugs](#reporting-bugs)
  - [Suggesting Enhancements](#suggesting-enhancements)
  - [Working on Issues](#working-on-issues)
  - [Creating Pull Requests](#creating-pull-requests)
- [Development Setup](#development-setup)
- [Testing](#testing)
- [Coding Standards](#coding-standards)
- [Community and Communication](#community-and-communication)
- [License](#license)

## Getting Started

### Fork the Repository

Either click the "Fork" button at the top right of the repository's page on GitHub OR create a fork on your local machine using `git clone`.

```bash
git clone https://github.com/datastarto/gravitino.git
cd gravitino
```

Now you are ready to start making contributions!

## Contribution Guidelines

### Code of Conduct

Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md). We are committed to providing a welcoming and inclusive environment for all contributors.

### Reporting Bugs

If you find a bug in the project, please open an issue on GitHub. Be sure to include as much detail as possible, such as a clear description, steps to reproduce, and your environment. Please follow the template provided.

### Suggesting Enhancements

If you have ideas for enhancements or new features, feel free to create an issue to discuss them. We welcome suggestions and will provide feedback on their feasibility and relevance.

### Working on Issues

Check out the list of open issues and find one that interests you. You can also assign an issue to yourself to indicate that you're working on it. Please keep the issue updated with your progress.

## Creating Pull Requests

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

After you have pushed your changes, create a pull request (PR) on our repository. Be sure to provide a detailed description of your changes, reference any related issues and please follow the template provided.

## Development Setup

Once you have cloned the repo [Github](https://github.com/datastrato/gravitino), see [how to build](/docs/how-to-build) for instructions on how to build, or you can use our docker images at [datastrato](https://hub.docker.com/u/datastrato) DockerHub repository.

To stop and start a local Gravitino server via ``bin/gravitino.sh start`` and ``bin/gravitino.sh stop`` in a Gravitino distribution, see [how to build](/docs/how-to-build) for more instructions.

## Testing

Our CI infrastructure will run unit and integration tests on each pull request, please make sure these tests pass before making a pull request.

We have unit tests that are run on every build and integration tests that can be run as needed. See [integration tests](docs/integration-test.md) for more in.

When adding new code or fixing a bug be sure to add unit tests to provide coverage.

## Coding Standards

We use Spotless to check code formatting, if your code is not correctly formatted then the build will fail. To correctly format your code please use Spotless.

```bash
./grawdlew spotlessApply
```

All files must have a license header and the build will fail if any files are missing license headers. If you are adding 3rd party code be sure to understand what needs to be listed in the LICENSE and NOTICE files.

For any bugs or new code please add unit tests to provide coverage of the code, code without unit tests may not be accepted.

## Community and Communication

Join our [community chat](https://datastrato-community.slack.com) to discuss ideas and seek help. We also encourage you to use github discussions and follow us on social media to stay updated on project news.

## License

By contributing to this project, you agree that your contributions will be licensed under the Apache License version 2. Please ensure that you have permission to do this if required by your employer.

Thank you for your contributions to gravitino! We appreciate your help in making this project a success.
