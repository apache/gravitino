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

# Legal documents for Maven artifacts

Maven JARs use the Apache 2.0 license and Gravitino notice in this directory.
The root `LICENSE.bin` and `NOTICE.bin` describe an entire server distribution,
so they must not be used for individual Maven artifacts.

`GenerateJarLegalFiles` adds notices for each module's copied production sources.
Source JARs have the same notices. Javadoc omits notices for private implementations
that are absent from the generated documentation, and points to the JDK doclet's
own `legal/` directory for its JavaScript and CSS licenses.
Root `LICENSE` records copied-source paths and root `NOTICE` includes their applicable
attributions. Copying Apache-2.0 code does not by itself require an additional NOTICE
entry: the copied Lance and Trino sources have no additional applicable upstream notice.

The Glue credentials provider derives from Doris's
[`CustomAwsCredentialsProvider`](https://github.com/apache/doris/blob/16da8a23b84985049be65b38f69d3e88fe477dbb/fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/s3tables/CustomAwsCredentialsProvider.java).
Gravitino commit [`d81dd65d1`](https://github.com/apache/gravitino/commit/d81dd65d1c9159dbb67d5ff18bac62c24e9ba39c)
explicitly recorded that adaptation in the source Javadoc and root `LICENSE`.
Commit [`2c930e827`](https://github.com/apache/gravitino/commit/2c930e8276f77c150e2cc328b37f17c5acca2643)
moved and reworked the existing class into `catalog-common`, removing the attribution
comment and old inventory paths. The current Doris entry preserves that provenance
at the new source location.

Bundled JARs preserve dependency documents under
`META-INF/licenses/<group>/<artifact>/<version>/`, retaining their original
relative paths. Metadata is generated as a ZIP so case-sensitive paths such as
`META-INF/LICENSE` and `META-INF/license/` survive on every host filesystem.
This avoids collisions between different `LICENSE`, `NOTICE`,
`LICENSE.txt`, and companion files. When a Gravitino runtime is bundled again,
its dependency documents keep that location. Connector runtimes that exclude
SLF4J classes also exclude the corresponding dependency documents.

`dependencies.txt` supplies verified texts missing from upstream binary JARs.
The canonical LICENSE lists each resource-backed component by its resolved group,
name and version, with exact document paths. External dependencies use Maven artifact
names; project dependencies use Gradle project names (for example, `api` rather than
the published `gravitino-api`). A label after `|` supplies its license
summary or additional requirements. Relevant NOTICE documents are also propagated
into the canonical NOTICE, with their original paths to resolve companion-file
references. Nested inventories are regenerated after dependency exclusions.
A version-specific `group:artifact:version` entry takes precedence over an
unversioned coordinate or group wildcard; this is necessary for Jackson's changing
embedded parser licenses.
The mapping applies only to dependencies included by Shadow's dependency filter, or the
CLI's runtime classpath. Ordinary thin, source and Javadoc JARs do not inherit
these dependencies' inventories. Review the entries when updating dependencies;
a POM license name alone does not account for embedded third-party code.

## Sources for supplements

- gRPC 1.66.0: [NOTICE](https://github.com/grpc/grpc-java/blob/v1.66.0/NOTICE.txt).
  Only the gRPC attribution applies to `grpc-api`; the okhttp/xds portions are
  not bundled. `grpc-context` is an empty compatibility JAR.
- Azure SDK: [Microsoft MIT license](https://github.com/Azure/azure-sdk-for-java/blob/azure-core_1.50.0/LICENSE.txt),
  verified for the Azure SDK dependencies in the Azure bundle.
- MSAL 1.16.1: [LICENSE](https://github.com/AzureAD/microsoft-authentication-library-for-java/blob/v1.16.1/LICENSE);
  persistence extension 1.3.0: [LICENSE](https://github.com/AzureAD/microsoft-authentication-extensions-for-java/blob/master/LICENSE).
- Legacy Azure keyvault-core 1.0.0: the exact Maven Central sources' `IKey.java`
  has Microsoft MIT terms; `IKeyResolver.java` is Apache 2.0.
- ASM 7.1/9.2/9.3: BSD terms in the exact sources' `AnnotationVisitor.java`.
- JSR305 3.0.2: BSD terms in the FindBugs distribution, plus the exact sources'
  `javax.annotation.concurrent` annotations, which retain Brian Goetz's
  CC-BY-2.5 attribution. [CC-BY-2.5 legal code](https://creativecommons.org/licenses/by/2.5/legalcode.en).
- Kotlin 1.9.10: [NOTICE](https://github.com/JetBrains/kotlin/blob/v1.9.10/license/NOTICE.txt),
  limited to the standard library attribution.
- JAXB 2.3.0: [LICENSE](https://github.com/javaee/jaxb-v2/blob/2.3.0/LICENSE).
- H2 2.2.224: [LICENSE](https://github.com/h2database/h2database/blob/version-2.2.224/LICENSE.txt).
- Jakarta Transactions 1.3.3: [LICENSE](https://github.com/jakartaee/transactions/blob/1.3.3/LICENSE.md)
  and [NOTICE](https://github.com/jakartaee/transactions/blob/1.3.3/NOTICE.md).
- Jackson Core 2.15.2 and 2.18.3: their exact source JARs contain Schubfach under
  MIT ([2.15.2 source](https://github.com/FasterXML/jackson-core/blob/jackson-core-2.15.2/src/main/java/com/fasterxml/jackson/core/io/schubfach/DoubleToDecimal.java),
  [2.18.3 source](https://github.com/FasterXML/jackson-core/blob/jackson-core-2.18.3/src/main/java/com/fasterxml/jackson/core/io/schubfach/DoubleToDecimal.java)),
  while the binary JARs omit that text. Their FastDoubleParser NOTICE declares
  MIT but the companion LICENSE incorrectly contains Apache 2.0. Supplements use
  the [0.9.0 MIT text](https://github.com/wrandelshofer/FastDoubleParser/blob/v0.9.0/LICENSE)
  and [1.0.90 MIT text](https://github.com/wrandelshofer/FastDoubleParser/blob/v1.0.90/LICENSE)
  respectively. Jackson 2.18.3 also needs the Boost text identified by its parser
  NOTICE. Existing fast_float and bigint texts are preserved. Newer Jackson
  versions with complete metadata do not receive these version-specific additions.
- Reactive Streams 1.0.4: [MIT-0 LICENSE](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.4/LICENSE).
- JaCoCo runtime 0.8.8: its binary `about.html` identifies EPL-2.0 and the
  embedded ASM 9.2 BSD code; retain that file (which already contains the ASM BSD
  text) and supplement only the missing full EPL-2.0 text.
- Snappy Java 1.1.10.8: [NOTICE](https://github.com/xerial/snappy-java/blob/v1.1.10.8/NOTICE).
  Its native libraries include [Snappy 1.1.10](https://github.com/google/snappy/blob/1.1.10/COPYING)
  and [Bitshuffle 0.3.4](https://github.com/kiyo-masui/bitshuffle/blob/0.3.4/LICENSE),
  as recorded by the tagged Makefile and VERSION file.
- Netty 4.1.109/110/118: [NOTICE](https://github.com/netty/netty/blob/netty-4.1.118.Final/NOTICE.txt)
  and [companion license texts](https://github.com/netty/netty/tree/netty-4.1.118.Final/license).
  The selected companions match across these versions. They cover embedded
  SLF4J/Harmony/JCTools in common, Webbit and Hoehrmann UTF-8 validation in HTTP,
  Bzip2/FastLZ/libdivsufsort/Protobuf in codec, HPACK implementations in HTTP/2,
  and Apple's dnsinfo header in the macOS native resolver. Optional external
  dependencies from Netty's omnibus NOTICE are not copied indiscriminately.
  Netty tcnative classes 2.0.65.Final use their own [2016 notice](https://github.com/netty/netty-tcnative/blob/netty-tcnative-parent-2.0.65.Final/NOTICE.txt) and Tomcat Native
  provenance, not Netty 4.1's 2014 notice. Build-wrapper and unbundled native-library
  sections are excluded from this Java-only artifact's supplement.
  The macOS supplement includes the full APSL-2.0 text and a source-availability link.
  The macOS-only dnsinfo inclusion follows [ASF LEGAL-613](https://issues.apache.org/jira/browse/LEGAL-613).

## Verification

`testMavenLegalFiles`, included in the root `check` task, checks that base templates
match the root documents and that copied-source notices recorded in `LICENSE` reach
the modules actually compiling those sources. It checks only sources with applicable
NOTICE templates, not every Apache-2.0 source. Small synthetic JARs exercise resource
selection, case-sensitive companions, classifier paths, supplement precedence,
empty compatibility JARs, nested exclusions, deterministic output and invalid mappings.
These checks do not build cloud bundles or connector runtimes.

The client-runtime tests check that its canonical LICENSE/NOTICE entries are unique,
that dependency documents are preserved, and that verified missing license texts,
component references and notices are present:

```shell
./gradlew testMavenLegalFiles :clients:client-java-runtime:test --tests '*TestRuntimeJarLegalFiles' -PskipITs
```

These focused tests do not establish that every dependency's licensing metadata is
complete. Check the actual Maven artifacts selected for a release, including nested
runtimes, thin/source/Javadoc JARs and cloud bundles. Review this mapping whenever
bundled dependencies change. Web WARs retain their existing legal documents.

Supplements are limited to missing material. Identical Microsoft texts are reused
across SDK/keyvault and MSAL/persistence components. JCTools and Netty's Apache-only
HPACK text are covered by the base Apache 2.0 license; no duplicate copies are added.
MIT-0 is retained to document the bundled component's license under ASF release
policy, even though MIT-0 itself has no attribution-retention condition.
