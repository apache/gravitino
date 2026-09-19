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

`LICENSE` and `NOTICE` contain the base text for Maven artifacts. Thin and source
JARs add applicable notices for copied production code. Javadoc JARs include notices
for material present in the documentation and reference the doclet's asset licenses.
Web WARs use their own legal documents.

Shaded and CLI JARs collect legal documents from their bundled dependencies under
`META-INF/licenses/<group>/<artifact>/<version>/`, preserving original relative
paths and optional classifiers. External dependencies use Maven artifact names;
project dependencies use Gradle project names. The main LICENSE lists these document
paths, and the main NOTICE includes upstream notices. Nested inventories are
regenerated after exclusions. A ZIP intermediate preserves case-sensitive paths.

`dependencies.txt` supplies missing upstream texts and license labels. Rules are
selected by `group:artifact:version`, then `group:artifact`, then `group:*`.
Comma-separated filenames precede an optional `|` license label. Rules apply only
to bundled dependencies with content; declarations in a POM do not trigger them.

When changing dependencies or copied sources, inspect the actual included code and
upstream legal documents. Update the relevant templates, mappings and root source
inventory. A dependency's POM license declaration may omit embedded third-party code.

## Sources for supplements

- gRPC 1.66.0: [NOTICE](https://github.com/grpc/grpc-java/blob/v1.66.0/NOTICE.txt).
  Only the gRPC attribution applies to `grpc-api`; the okhttp/xds portions are
  not bundled. `grpc-context` is an empty compatibility JAR.
- Azure SDK: [Microsoft MIT license](https://github.com/Azure/azure-sdk-for-java/blob/azure-core_1.50.0/LICENSE.txt),
  shared by the Azure SDK components in the Azure bundle.
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
  NOTICE. Existing fast_float and bigint texts are preserved.
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
  and Apple's dnsinfo header in the macOS native resolver.
  Netty tcnative classes 2.0.65.Final use the Java artifact's Netty and Tomcat Native
  attributions from its [NOTICE](https://github.com/netty/netty-tcnative/blob/netty-tcnative-parent-2.0.65.Final/NOTICE.txt).
  The macOS supplement includes the full APSL-2.0 text and a source-availability link.
  The macOS-only dnsinfo inclusion follows [ASF LEGAL-613](https://issues.apache.org/jira/browse/LEGAL-613).

## Copied-source provenance

Root `LICENSE` records copied-source paths; root `NOTICE` and the module mappings
identify applicable notices. The Glue provider in `catalog-common` derives from
[Doris's provider](https://github.com/apache/doris/blob/16da8a23b84985049be65b38f69d3e88fe477dbb/fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/s3tables/CustomAwsCredentialsProvider.java),
with provenance recorded in [the original attribution](https://github.com/apache/gravitino/commit/d81dd65d1c9159dbb67d5ff18bac62c24e9ba39c).
The Ranger reference classes use the attribution from [Ranger 2.4.0](https://github.com/apache/ranger/blob/release-ranger-2.4.0/NOTICE.txt).

## Verification

The root `check` task includes `testMavenLegalFiles`, which checks template and source
mapping consistency and exercises the generator with synthetic JARs. Client-runtime
tests check the actual shaded JAR's canonical entries, dependency documents,
supplements and references.

```shell
./gradlew testMavenLegalFiles :clients:client-java-runtime:test --tests '*TestRuntimeJarLegalFiles' -PskipITs
```

Before a release, inspect the final Maven artifacts and review metadata for changed
dependencies. These tests check packaging behavior, not completeness of upstream
licensing information.
