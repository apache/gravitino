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

Bundled JARs preserve dependency documents under
`META-INF/licenses/<group>/<artifact>/<version>/`, retaining their original
relative paths. This avoids collisions between different `LICENSE`, `NOTICE`,
`LICENSE.txt`, and companion files. When a Gravitino runtime is bundled again,
its dependency documents keep that location. Connector runtimes that exclude
SLF4J classes also exclude the corresponding dependency documents.

`dependencies.txt` supplies verified texts missing from upstream binary JARs.
An optional label after `|` identifies the product, selected license and homepage
for dependencies with additional requirements. These labels appear prominently
in the artifact's canonical LICENSE, including when inherited from a nested runtime.
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
- Reactive Streams 1.0.4: [MIT-0 LICENSE](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.4/LICENSE).
- JaCoCo runtime 0.8.8: its binary `about.html` identifies EPL-2.0 and the
  embedded ASM 9.2 BSD code; retain that file as well as the full license texts.
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
  The macOS-only dnsinfo inclusion follows [ASF LEGAL-613](https://issues.apache.org/jira/browse/LEGAL-613).

## Optional OpenSSL provider

The AWS and Azure bundles exclude WildFly OpenSSL 1.1.3.Final. Its
`DirectByteBufferDeallocator` implementation is LGPL-2.1-or-later despite the
artifact's Apache-only POM. Hadoop's default modes use JSSE or fall back to JSSE
when this optional provider is unavailable. Explicit OpenSSL mode requires a
separately installed provider. The artifact regression test exercises Hadoop's
JSSE fallback using the actual shaded bundles.

## Verification

The default client-runtime tests check its canonical documents and preservation
of dependency legal resources, including Jackson's companion texts. The broader
audit builds thin/source/Javadoc, CLI, nested filesystem runtime and cloud JARs:

```shell
./gradlew :clients:client-java-runtime:test --tests '*TestRuntimeJarLegalFiles' -PcheckMavenLegalFiles -PskipITs
```

The cloud check verifies JSSE factory initialization without WildFly, not a
network handshake or cloud operation. Final staged artifacts still require the
usual release verification.
