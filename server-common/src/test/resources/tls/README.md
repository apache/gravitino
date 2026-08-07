# TLS Integration Test Fixtures

This directory contains static PKCS12 keystore and truststore fixtures used by the TLS integration tests.

These files are committed to the repository so the tests can perform real TLS and mutual TLS handshakes.

## Test-only warning

All certificates, private keys, aliases, and passwords in this directory are for testing only.

They are intentionally stored in the repository and must not be used in production or for any security-sensitive purpose.

## Passwords

All PKCS12 stores use the following password:

```text
changeit
```

Private-key entries also use:

```text
changeit
```

The same password is used for all fixtures to keep the integration-test configuration simple.

## Fixture files

| Filename                               | Purpose                                                                                                                         | Alias                   | Certificate subject                                                                             |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | ----------------------- | ----------------------------------------------------------------------------------------------- |
| `test-server-keystore.p12`             | Contains the private key and certificate presented by the Jetty HTTPS server.                                                   | `test-server`           | `CN=localhost, OU=TLS Integration Tests, O=Apache Gravitino, L=Test, ST=Test, C=US`             |
| `test-server-truststore.p12`           | Contains the trusted client certificate used by the server during mutual TLS authentication.                                    | `test-trusted-client`   | `CN=test-trusted-client, OU=TLS Integration Tests, O=Apache Gravitino, L=Test, ST=Test, C=US`   |
| `test-client-truststore.p12`           | Contains the server certificate trusted by the HTTPS client.                                                                    | `test-server`           | `CN=localhost, OU=TLS Integration Tests, O=Apache Gravitino, L=Test, ST=Test, C=US`             |
| `test-trusted-client-keystore.p12`     | Contains the private key and certificate for the client that should be accepted by the server during mutual TLS authentication. | `test-trusted-client`   | `CN=test-trusted-client, OU=TLS Integration Tests, O=Apache Gravitino, L=Test, ST=Test, C=US`   |
| `test-untrusted-client-keystore.p12`   | Contains a client private key and certificate that is not trusted by the server and should cause the TLS handshake to fail.     | `test-untrusted-client` | `CN=test-untrusted-client, OU=TLS Integration Tests, O=Apache Gravitino, L=Test, ST=Test, C=US` |
| `test-untrusted-server-truststore.p12` | Contains an unrelated trusted certificate, causing the client to reject the test server certificate.                            | `test-unrelated-server` | `CN=test-unrelated-server, OU=TLS Integration Tests, O=Apache Gravitino, L=Test, ST=Test, C=US` |

## Server certificate hostname

The test server is accessed using:

```text
https://localhost:<port>
```

The server certificate must therefore contain the following Subject Alternative Name:

```text
DNS:localhost
```

## Expected entry types

Identity keystores should contain a private-key entry:

```text
Entry type: PrivateKeyEntry
```

This applies to:

* `test-server-keystore.p12`
* `test-trusted-client-keystore.p12`
* `test-untrusted-client-keystore.p12`

Truststores should contain trusted certificate entries:

```text
Entry type: trustedCertEntry
```

This applies to:

* `test-server-truststore.p12`
* `test-client-truststore.p12`
* `test-untrusted-server-truststore.p12`

## Inspecting a fixture

Use the following command to inspect a PKCS12 fixture:

```bash
keytool -list -v \
  -storetype PKCS12 \
  -keystore test-server-keystore.p12 \
  -storepass changeit
```

Replace the filename as needed.

The output should be used to verify:

* alias name
* entry type
* certificate subject
* certificate issuer
* certificate validity dates
* Subject Alternative Names
* certificate chain

## Regeneration requirements

When regenerating the fixtures, preserve the following requirements:

1. All stores must use the PKCS12 format.
2. All store passwords must be `changeit`.
3. All private-key passwords must be `changeit`.
4. The server certificate must include `DNS:localhost` as a Subject Alternative Name.
5. The trusted client certificate must be present in the server truststore.
6. The server certificate must be present in the client truststore.
7. The untrusted client certificate must not be present in the server truststore.
8. The untrusted server truststore must not contain the test server certificate or any certificate authority that issued it.
9. Trusted and untrusted identities must use different private keys and certificates.
10. The filenames, aliases, and certificate subjects documented above must be updated if they change.

## Regeneration instructions

The fixtures can be regenerated using the JDK `keytool` utility.

Run the fixture-generation script from this directory:

```bash
./regenerate.sh
```

The script recreates all PKCS12 keystores and truststores using the passwords, aliases, subjects, and trust relationships documented above.

After regeneration, inspect each fixture using:

```bash
keytool -list -v \
  -storetype PKCS12 \
  -keystore <fixture-name>.p12 \
  -storepass changeit
```

The generated certificates are valid for 3,650 days (10 years) and must be regenerated before they expire.

## Running the integration test

From the repository root, run:

```bash
./gradlew :server-common:test --tests "org.apache.gravitino.server.web.TestJettyServer"
```
