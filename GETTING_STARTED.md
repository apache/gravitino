# Getting Started With Apache Gravitino

## 1. Introduction

Welcome to Apache Gravitino! This guide helps new contributors get started quickly. It covers cloning the repository, building the project, submitting your first pull request, and contributing in any form.

---

## 2. Prerequisites

Before building Gravitino, ensure you have:

- Linux or macOS operating system
- Git installed
- Java Development Kit (JDK) 17 installed to launch Gradle
- Python 3.8–3.12 to build the Gravitino Python client
- Optionally, Docker to run integration tests

Full build instructions: [Gravitino How to Build](https://gravitino.apache.org/docs/how-to-build/)

---

## 3. Clone the Repository

```bash
git clone https://github.com/apache/gravitino.git
cd gravitino
```

---

## 4. Create a Branch

Create a branch for your work:

```bash
git checkout -b <your-branch>
```

---

## 5. Build the Project

Build Gravitino:

```bash
./gradlew build
```

- CI runs **Spotless** formatting checks and all unit tests
- New files must include ASF headers
- Gradle detects and downloads the necessary JDK automatically

---

## 6. Good First Issues

Check out [Good First Issues](http://github.com/apache/gravitino/contribute) to start contributing with manageable tasks.

---

## 7. Making Changes

- Make changes in your branch
- Commit changes with clear messages
- Ensure all tests pass and Spotless formatting have been done

```bash
./gradlew spotlessApply
./gradlew test
```

- All forms of contribution are welcome—not just code

---

## 8. Submit a Pull Request

- Push your branch to GitHub:

```bash
git push origin <your-branch>
```

- Open a pull request on the Gravitino repository
- Link to relevant issue

---

## 9. Community Etiquette

- Respect other contributors
- Follow our [Code of Conduct](https://github.com/apache/gravitino/blob/main/CODE_OF_CONDUCT.md)
- Engage constructively in PR reviews and discussions

---

## 10. Documentation

Project documentation is available here: [Gravitino Docs](https://gravitino.apache.org/docs)  

---

## 11. First PR Checklist

Before submitting your first PR:

- [ ] Build succeeds locally
- [ ] Spotless formatting applied
- [ ] Unit tests pass
- [ ] ASF headers added to new files
- [ ] PR targets a new branch `<your-branch>`
- [ ] Subject line includes reference to the issue
