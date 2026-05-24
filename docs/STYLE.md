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

# Documentation Style Guide for Apache Gravitino

Style and review guidance for human and AI contributors editing Gravitino documentation, log messages, exception messages, and configuration descriptions. When reviewing existing text, propose edits one at a time, tag each with a category, and make no changes without explicit approval.

## Project names and capitalization

Always capitalized in prose. These are proper nouns:

- **Gravitino**, **Apache Gravitino** (never "gravitino" or "Grivitino" in prose; lowercase `gravitino` is only correct inside literal config keys like `gravitino.uri`)
- **Iceberg**, **Apache Iceberg**
- **Hive**, **Apache Hive** (lowercase "metastore" when used as a generic noun: "the Hive metastore")
- **Paimon**, **Apache Paimon**
- **Trino**, **Spark**, **Flink**, **Kafka**, **Hadoop**
- **Ranger**, **Apache Ranger**
- **Polaris**, **Nessie**
- **Lance**, **Doris**
- **Datastrato**

Code identifiers (class names, method names, config keys, CLI flags) stay in their literal source casing inside backticks. Do not "correct" `gravitino.uri` to `Gravitino.uri`.

## Terminology

- **metalake** — one word, lowercase. Not "meta lake", not "Metalake".
- **catalog**, **schema**, **table**, **view**, **fileset**, **topic**, **model** — lowercase concept names.
- **REST catalog**, **Iceberg REST catalog**, **IRC** — initialism uppercased.
- **federated metadata catalog** or **data catalog** in marketing-adjacent prose. Avoid "data catalog platform".
- Prefer **engine** over "compute" when referring to query engines (Trino, Spark, Flink).
- Prefer **credential vending** over "credential issuing" or "credential generation" in IRC context.
- **OSI** stands for Open Semantic Catalog in Gravitino docs, not Open Systems Interconnection.
- Use **pushdown** (one word) for the noun and adjective forms ("supports pushdown", "predicate pushdown"). Reserve "push down" (two words) for the rare phrasal-verb use ("the engine pushes the filter down"). Do not use the hyphenated "push-down".
- Verb vs. noun: "shut down" / "back up" / "set up" / "log in" / "log out" are verbs (two words). "Shutdown" / "backup" / "setup" / "login" / "logout" are nouns (one word). Match the form to the grammatical use.
- Hyphenate compound adjectives before nouns: "in-memory cache", "real-time updates", "long-running operation", "open-source project". When the same phrase is used as a predicate adverbial ("the data is stored in memory"), no hyphen.
- **Docker**, **Kubernetes**, **Linux**, **Python**, **Java**, **MySQL**, **PostgreSQL**, and other product/language names are always capitalized in prose. Lowercase forms in code, file paths, and config keys are technically meaningful and should not be changed.

## Initialisms

Always uppercase when used as identifiers in prose:

- **ID**, **URI**, **URL**, **API**, **REST**, **HTTP**, **HTTPS**, **JSON**, **YAML**, **CSV**, **TSV**
- **SQL**, **JDBC**, **ODBC**
- **AWS**, **GCP**, **GCS**, **S3**, **IAM**, **STS**, **OCI**, **ARN**
- **OAuth**, **OAuth2**, **OIDC**, **LDAP**, **SCIM**, **SAML**, **JWT**, **SSO**, **AD**
- **SLF4J**, **UGI**, **HDFS**, **YARN**, **K8s** (or **Kubernetes** spelled out)
- **SPI** (often misspelled as "SIP"), **SDK**, **CLI**, **UI**, **CI**, **CD**, **GA**, **POC**, **PRD**
- **MCP** (Model Context Protocol)

Inside code samples and config keys, follow the literal source.

## Log, exception, and error message style

Apply these rules when reviewing or writing messages:

- "Can not" / "can not" → "Cannot" / "cannot"
- "Fail to X" → "Failed to X" (past-tense participle when describing a completed failure)
- Drop trailing periods from log and exception message strings. Single-sentence messages do not need terminal punctuation.
- Capitalize the first word of the message.
- Use SLF4J `{}` parameters over string concatenation. When the last argument is an exception, SLF4J auto-formats it. Avoid patterns like `LOG.warn("error: {}", roleName, e)` where `e` is appended as a parameter.
- Avoid "the" before bare proper nouns. "in Ranger" not "in the Ranger".
- Avoid the "operate object [%s] operation [%s]" pattern. Prefer "perform [%s] on [%s]" or similar.
- Be specific about what failed. "Failed to connect to Hive metastore at {}" beats "Connection error".

## Documentation prose style

- Voice: clear, direct, technical. Active over passive where natural. Address the reader as an engineer ("Configure the catalog by..." rather than "The catalog can be configured by...").
- Avoid abstract-subject framings that flatten the action: "The goal of X is to Y" → "X aims to Y"; "The purpose of X is to Y" → "X..."; "It is recommended that you Y" → "Y" or "Y is recommended"; "X is designed to Y" / "X is used to Y" → "X does Y" when natural.
- Drop "you can / you need to / you should" scaffolding. The imperative is shorter and matches engineering voice. "You can create a catalog by sending a POST request" -> "Create a catalog by sending a POST request"; "You need to provide the following" -> "Provide the following". Use "you" only when you really mean to address the reader (for example, conditionals like "if you want to...") — don't pad an instruction with it.
- Cut filler: "in order to" → "to", "due to the fact that" → "because", "at this point in time" → "now", "make use of" → "use".
- Drop "please" before imperative verbs in technical prose ("please see X" → "see X", "please use X" → "use X"). The exceptions are user-facing strings (error messages, UI copy) embedded in code samples where polite tone is intentional. Apply broadly, not just to "please refer to" / "please see".
- Avoid "currently" and "the current behavior". Docs describe the current state by default; the qualifier adds nothing. If something is genuinely about to change, name the version where it changes ("In 1.0, only X is supported; built-in support is planned for a future release") rather than handwaving with "currently". Adjectival uses are fine ("the currently selected item"). The rule applies to redundant filler only — when the word conveys actual time-bound meaning ("this feature only exists now, may change later"), keep it or rephrase to a clearer time qualifier ("at present", "in this release").
- Use the Oxford comma.
- No em dashes. Use commas, parentheses, or two separate sentences.
- Avoid starting sentences with "This" as a bare pronoun. Use "This [noun]" with an explicit referent, or restructure.
- Headings use Title Case at H1 through H4. Capitalize the first word, the last word, and all nouns, verbs, adjectives, adverbs, and pronouns. Lowercase articles (a, an, the), coordinating conjunctions (and, but, or, nor), and prepositions of four letters or fewer (with, in, for, by, to, of, at, on) unless they are the first or last word of the heading. Use sentence case at H5 and below if any exist.
- Avoid gerund-led headings. Rare exceptions are acceptable for well-established phrases like "Getting started" that have no clean noun-phrase equivalent. When in doubt, use a noun phrase.
- Parallel structure: when a heading pairs two or more verbs with "and", use the same verb form for both. Prefer the imperative ("Build and Test", not "Build and Testing").
- Avoid second-person possessives ("your X") in headings, consistent with the broader rule against second-person scaffolding. Use "the X" or drop the determiner.
- Avoid "Example of <gerund-ing> X" headings. When the section walks the reader through doing something, use a specific-imperative heading instead ("Configure X", "Create X", "Connect X").
- Avoid "X of (the) Y" prepositional-phrase headings. Use the attributive form "Y X" instead ("Job System Configuration", not "Configurations of the Job System"). Prefer singular over plural for the X noun unless the section enumerates multiple distinct instances.
- Headings should not use the possessive (singular `'s` or plural `s'`). Use an attributive noun ("Service Configuration", "Catalog Information") or a prepositional phrase ("Configuration of the Service") instead.
- No terminal punctuation in headings. Headings do not end with a period, colon, exclamation mark, or other terminal punctuation. The content below the heading speaks for itself.
- Question-form headings ("What is X?", "Why use Y?") belong in FAQ sections only. For sections introducing or describing something, use a declarative form ("Overview", "Introduction", "Architecture", "Benefits"). Terminal "?" is allowed only in FAQ contexts.
- No fragment headings starting with prepositions or conjunctions. Headings like "With X", "Without X", "For X", "From X", "And X", "But X", "Or X" implicitly attach to a subject elsewhere and read as fragments. Prefer a noun phrase ("X", "X configuration", "No X") or an imperative ("Configure X").
- Don't repeat the leading verb across consecutive bullets ("Supports X" / "Supports Y" / "Supports Z"). Either lift the verb into a shared lead-in ("The catalog supports:" followed by noun-phrase bullets) or vary the verb so each bullet says something distinct. Identical leading verbs are noise — readers skim the difference, not the repetition.
- A sentence that introduces a numbered or bulleted list ends with a colon, not a period. When the sentence references the list forward with "the X", use "these X" instead.
- Avoid the tautological qualifier "created" in operation descriptions ("list the created groups", "returns the created tables"). Listing or retrieving an entity that doesn't exist is impossible, so the qualifier adds no information. Use the bare noun: "list the groups", "returns the tables".
- One sentence per line is encouraged for editability when it reads naturally. Never cram two sentences onto one line.
- Numbers: spell out one through nine, use digits for 10 and above. Exception: numbers paired with units (5 GB, 3 ms) always digits.
- Time: "5 ms", "30 s", "2 min", "1 hour" with a space.

## Page structure

Page title convention: every doc uses the frontmatter `title:` field as its page title. Docusaurus renders this as the rendered page H1. Body markdown must not contain any `# H1` headings — they create duplicate page titles. All section headings in the body start at H2 (`##`).

## What to leave alone

These are out of scope for prose review. Do not edit without explicit approval:

- **Fenced code blocks** (```), inline `code` in backticks, JSON / YAML / TOML snippets. Even if a JSON value contains a typo, flag it for the human; do not change it.
- **Frontmatter** at the top of Markdown files (YAML between `---` markers).
- **Link URLs** and anchor targets. Link display text is fair game; the URL is not.
- **Property names**, config keys, CLI flags, environment variables (`GRAVITINO_HOME`, `gravitino.uri`, `--catalog-name`).
- **Class names, method names, package paths** in their source casing.
- **Generated content markers** and any block between `<!-- generated -->` and `<!-- /generated -->`.
- **Author names, contributor handles, GitHub usernames.**

## Review workflow

When reviewing a document, follow this pattern:

1. Read the whole file before proposing any edits so suggestions reflect the document's structure and arc.
2. Propose edits one at a time. Each proposal is a single conceptual change with `old_text`, `new_text`, a category, and a brief reason.
3. Categories: `typo`, `spelling`, `grammar`, `clarity`, `accuracy`, `style`, `terminology`, `consistency`.
4. For `accuracy` proposals (claims about how Gravitino behaves), do not assert the doc is wrong. Flag it as needing engineering verification: "Doc says default port is 9083, confirm with engineering before changing."
5. Do not bundle unrelated edits in the same proposal. One conceptual change per accept/reject decision.
6. After all proposals in a file are reviewed, draft a conventional-commit message: `docs(<area>): <improvement>`.
7. When applying a mechanical pattern sweep, if the resulting sentence reads awkwardly (comma splice, dangling fragment, missing connector, awkward parallel structure), fix the whole sentence in the same commit rather than leaving cleanup for a later pass. The goal is that any sentence the sweep touches reads cleanly after the commit lands.
8. Run a typo and misspelling pass as part of the polish workflow, especially after sweeps that strip leading words (which can expose misspelled words that were partially hidden in the original phrasing).

## Priority of issues

Edits are not equally important. In rough descending order of priority:

1. **Coherence and meaning.** Sentences that don't parse, missing subjects
   or verbs, run-ons that fuse three ideas, paragraphs where the topic
   shifts without warning. These are the highest-value fixes.
2. **Structure and flow.** Choppy sequences of short declarative sentences
   that read like bullet points in prose, missing transitions between
   paragraphs, lists that should be prose or prose that should be a list,
   missing or wrong headings for a region of text.
3. **Clarity and voice.** Passive constructions where active would be
   cleaner, filler phrases, vague subjects ("It is important to note
   that..."), instructions that don't clearly tell the reader what to do.
4. **Grammar and idiom.** Subject-verb agreement, tense consistency,
   article usage, idiomatic phrasing.
5. **Style and terminology.** The conventions captured elsewhere in this file.
6. **Spelling and typos.** Lowest priority because they're rarely the
   actual problem with bad docs.

Lead with high-priority issues. Don't bury a paragraph-level rewrite below
twelve typo fixes.

## Edit scale

A single "atomic edit" can be any of:

- A word ("Grivitino" → "Gravitino")
- A phrase ("would better use" → "should use")
- A sentence (rewrite a choppy or incoherent sentence)
- A paragraph (restructure for flow, split mixed concerns)
- A section (substantial rewrite of an unclear region)

Bigger edits get longer reasons. For a paragraph rewrite, the reason
should explain what was wrong with the original (mixed concerns, choppy
rhythm, unclear referent, missing transition) so the reviewer can
evaluate whether the proposed structure actually solves the problem.

## PR boundaries

When grouping reviewed files into PRs, prefer topical bundles over arbitrary chunks. Examples:

- Authentication and authorization docs together
- All getting-started and quickstart docs together
- Per-catalog docs grouped by catalog (Iceberg, Paimon, Hive, JDBC, Lakehouse)
- IRC and REST service docs together
- Trino, Spark, Flink engine connector docs together

Aim for 5-15 files per PR. PR title format: `docs(<area>): <improvement>`. PR description should list the categories of changes and call out anything that needed engineering verification.

## Notes for future maintainers of this file

Update this guide whenever a review session surfaces a new convention worth codifying. The goal is to drive the per-file review rejection rate down over time. Style decisions made repeatedly should become rules here.
