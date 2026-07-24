---
title: "Apache Gravitino Trino connector with Starburst"
slug: /trino-connector/starburst-compatibility
keyword: gravitino connector trino starburst
license: "This software is licensed under the Apache License version 2."
---

This page describes, **for indication only**, how you can build the Apache Gravitino
Trino connector against the **Starburst** SPI — a commercial, proprietary product —
instead of the open-source Trino SPI.

:::caution Read this first — this is your responsibility, not Gravitino's
Apache Gravitino is an open-source project licensed under the Apache License 2.0, and
it builds **exclusively against the open-source `io.trino:trino-spi` artifacts** published
on Maven Central.

**Starburst Enterprise / Starburst Galaxy are commercial, proprietary products.** Their
`trino-spi` jar is **not** an Apache artifact, is **not** redistributable by Gravitino, and
is **not** published to Maven Central. For this reason the Gravitino project:

- does **not** ship, bundle, or depend on any Starburst artifact,
- does **not** officially support or test the connector against Starburst,
- cannot commit any of the build changes described below into the repository.

If you choose to build the connector against a Starburst SPI, you do so **entirely within your own private usage**,
under the terms of the Starburst license you
already hold for your deployment. You are responsible for obtaining the jar legitimately from
your own licensed Starburst installation and for complying with its license.
:::

:::note This document is agnostic and purely indicative
Although the author of this documentation has performed a successful test for a specific Starburst
and Trino version between 473 and 478, every value below — version numbers, jar file names,
file system paths, and the exact set of transitive dependencies you must re-declare — **depends on your
specific Starburst release and deployment** and **will change over time**. Treat the snippets
as a starting point to adapt to your environment, not as a guaranteed, supported recipe.
Nothing here is globally tested or maintained by the Gravitino project, unless Starburst users
volunteer to maintain it.
:::

## Why this is possible at all

The shared connector source code is written to stay **cross-version compatible**. Where a method
exists only in some Trino/Starburst SPI variants (for example
`DynamicFilter#getPreferredDynamicFilterTimeout()`), it is implemented **without** the `@Override`
annotation so the source compiles against SPI versions that do not declare it, while still being
dispatched at runtime by signature on SPI versions that do. This means that, in most cases, you only
need to **swap the SPI dependency** at build time — **no source code modification is required** —
only a few build-time changes, described below.

## Steps

The example below targets the `trino-connector-473-478` version-segment module and a Starburst
SPI version `476-e.0.26`. Pick the [version-segment module](development.md#multi-version-architecture)
that matches your Starburst-based Trino version, and replace the version/file names accordingly.

### 1. Obtain the Starburst `trino-spi` jar

Retrieve the `io.trino_trino-spi-<starburstVersion>.jar` from your own licensed Starburst
deployment. The exact location depends on how Starburst was installed — for a typical server
install it is found under the Starburst libraries directory (for example,
`/usr/lib/starburst/lib/`), or inside the distribution tarball/RPM you deployed from.

:::note
The jar name and version suffix (`-e.0.x`, etc.) are Starburst-specific and vary by release.
Use whatever name your deployment actually provides.
:::

### 2. Place the jar in a local folder

Create a folder at the root of the Gravitino project and copy the jar into it:

```shell
mkdir -p starburst-libs
cp /usr/lib/starburst/lib/io.trino_trino-spi-476-e.0.26.jar starburst-libs/
```

:::caution
Do **not** commit this folder or the jar. It is a proprietary artifact for your private build only.
Consider adding `starburst-libs/` to your local `.gitignore`.
:::

### 3. Point the version-segment module at the Starburst jar

Edit the `build.gradle.kts` of your chosen version-segment module (here
`trino-connector/trino-connector-473-478/build.gradle.kts`).

**Remove** the open-source SPI dependency:

```kotlin
compileOnly("io.trino:trino-spi:$trinoVersion") {
  exclude("org.apache.logging.log4j")
}
```

**Add** the local Starburst jar instead, plus any transitive dependency the raw jar no longer
brings in. A file dependency carries no POM, so transitive libraries such as `io.airlift:slice`
must be declared explicitly:

```kotlin
compileOnly(files("../starburst-libs/io.trino_trino-spi-$starburstVersion.jar"))
compileOnly("io.airlift:slice:0.46")
```

:::note
`io.airlift:slice` is one common example. Depending on your Starburst SPI version you may need
to re-declare additional `compileOnly` dependencies that the published `trino-spi` POM used to
provide transitively. Add them as the compiler reports missing symbols.
:::

### 4. Declare the `starburstVersion` build property

Still in the same `build.gradle.kts`, read the property so the snippet above can resolve
`$starburstVersion`:

```kotlin
val starburstVersion: String = project.properties["starburstVersion"] as String
```

### 5. Build with the Starburst version argument

Pass the property on the Gradle command line:

```shell
./gradlew :trino-connector:trino-connector-473-478:build -PstarburstVersion=476-e.0.26
```

:::note
Each version-segment module still validates the `-PtrinoVersion` range. If the module rejects your
Starburst-based version number, set a `-PtrinoVersion` value inside the supported range of the
module you selected, since the actual SPI now comes from the local jar.
:::

## Disclaimer

This integration path is provided as community guidance only. It is **not** part of the Apache
Gravitino release, is **not** covered by Gravitino support, and may break with any Starburst or
Gravitino update. Validate the resulting connector thoroughly in your own environment before any
use.
