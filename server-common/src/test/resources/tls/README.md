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

# TLS test certificates

`TestTlsServerUtils.generateTlsMaterial(Path)` generates self-signed certificates
and PKCS12 stores using `keytool` from the JDK running the tests.

`TestHttpsServerAuthentication` and `TestHTTPClientTLS` generate the files once
per test class in a JUnit `@TempDir`. JUnit removes the directory after the class
finishes. No committed certificate files or manual regeneration are needed.

All store and private-key passwords are `changeit`, for testing only. The server
certificate covers `localhost` and `127.0.0.1`. Certificates are valid for 30 days
from generation.

## Generated stores

| File | Contents |
| --- | --- |
| `test-server-keystore.p12` | Server private key and certificate. |
| `test-trusted-client-keystore.p12` | Trusted client private key and certificate. |
| `test-untrusted-client-keystore.p12` | Separate client identity that the server does not trust. |
| `test-client-truststore.p12` | Server certificate trusted by the client. |
| `test-server-truststore.p12` | Trusted client certificate accepted by the server. |
| `test-untrusted-server-truststore.p12` | Unrelated certificate, used to test server certificate rejection. |

The server truststore excludes the untrusted client certificate. The untrusted
server truststore excludes the server certificate. Exported `.cer` files are
also kept in the temporary directory.

## Run the tests

From the repository root:

```bash
./gradlew :server-common:test \
  --tests '*TestTlsMaterialGeneration' \
  --tests '*TestHttpsServerAuthentication' \
  :clients:client-java:test \
  --tests '*TestHTTPClientTLS' \
  -PskipITs
```
