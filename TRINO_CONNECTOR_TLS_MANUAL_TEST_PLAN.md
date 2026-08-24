# Trino Connector Internal JDBC TLS and Session Role Manual Test Plan

## 1. Scope

This branch adds TLS and session role support to the internal JDBC connection used by the Gravitino Trino Connector. The connector uses this connection to connect back to the Trino coordinator and execute `CREATE CATALOG` and `DROP CATALOG` statements.

The manual test covers:

- Backward compatibility with a plaintext HTTP coordinator.
- Automatic TLS detection from an HTTPS `discovery.uri`.
- Explicit TLS configuration.
- `FULL`, `CA`, and `NONE` certificate verification modes.
- Truststore configuration and validation.
- Session roles used for privileged catalog operations.
- Arbitrary Trino JDBC driver properties.
- Default HTTP and HTTPS port derivation.
- Configuration validation and error reporting.
- Protection of JDBC credentials and other sensitive properties.

The main implementation files are:

- `trino-connector/trino-connector/src/main/java/org/apache/gravitino/trino/connector/GravitinoConfig.java`
- `trino-connector/trino-connector/src/main/java/org/apache/gravitino/trino/connector/catalog/CatalogRegister.java`
- `docs/trino-connector/configuration.md`

## 2. Test Environment

Use the existing Trino Connector integration environment as the baseline:

`trino-connector/integration-test/trino-test-tools/trino-cascading-env/docker-compose.yaml`

Prepare the following components:

- A Trino coordinator running the connector built from this branch.
- A Gravitino server accessible from the Trino container.
- A simple catalog that Gravitino can create dynamically, such as a JDBC MySQL catalog.
- An HTTP Trino endpoint for compatibility testing.
- An HTTPS Trino endpoint using a self-signed or private-CA certificate.
- A truststore that trusts the Trino coordinator certificate.
- A truststore that does not trust the Trino coordinator certificate.
- Invalid truststore inputs: a missing path, wrong password, and wrong type.
- Trino access-control configuration in which `CREATE CATALOG` and `DROP CATALOG` require a privileged system role.

For every successful connection scenario, exercise the complete catalog lifecycle:

1. Start the Trino coordinator with the target Gravitino catalog configuration.
2. Create a catalog in Gravitino.
3. Confirm that the catalog appears in `SHOW CATALOGS` in Trino.
4. Run `SHOW SCHEMAS FROM <catalog>` or an equivalent simple query.
5. Delete the catalog from Gravitino.
6. Confirm that the catalog disappears from `SHOW CATALOGS`.

## 3. Manual Test Plan

### 3.1 HTTP Backward Compatibility

Configure Trino with:

```properties
discovery.uri=http://trino-host:8080
```

Do not set any `trino.jdbc.ssl.*` property in the Gravitino catalog configuration.

Expected results:

- The Gravitino Connector starts successfully.
- The internal JDBC connection remains plaintext.
- Dynamic catalog creation, query, and deletion succeed.
- No TLS or JDBC initialization error appears in the logs.

### 3.2 Automatic TLS Detection

Configure Trino with:

```properties
discovery.uri=https://trino-host:8443
```

Configure the Gravitino catalog with:

```properties
trino.jdbc.ssl.truststore.path=/etc/trino/truststore.jks
trino.jdbc.ssl.truststore.password=changeit
trino.jdbc.ssl.truststore.type=JKS
```

Do not set `trino.jdbc.ssl.enabled`.

Expected results:

- The connector derives TLS enablement from the HTTPS scheme.
- The internal JDBC connection completes certificate verification.
- The complete dynamic catalog lifecycle succeeds.

### 3.3 Explicit TLS Enablement

Add the following properties:

```properties
trino.jdbc.ssl.enabled=true
trino.jdbc.ssl.verification=FULL
trino.jdbc.ssl.truststore.path=/etc/trino/truststore.jks
trino.jdbc.ssl.truststore.password=changeit
trino.jdbc.ssl.truststore.type=JKS
```

Expected results:

- The connector starts successfully.
- The complete dynamic catalog lifecycle succeeds over TLS.

### 3.4 Certificate Verification Modes

#### FULL

Use a trusted certificate whose hostname matches the JDBC target.

Expected result: the connection and catalog lifecycle succeed.

Repeat with a trusted certificate whose hostname does not match the JDBC target.

Expected result: connection initialization fails because hostname verification fails.

#### CA

Configure:

```properties
trino.jdbc.ssl.enabled=true
trino.jdbc.ssl.verification=CA
trino.jdbc.ssl.truststore.path=/etc/trino/truststore.jks
trino.jdbc.ssl.truststore.password=changeit
```

Use a trusted certificate whose hostname does not match the JDBC target.

Expected result: the connection and catalog lifecycle succeed because the CA is verified without hostname verification.

#### NONE

Configure:

```properties
trino.jdbc.ssl.enabled=true
trino.jdbc.ssl.verification=NONE
```

Do not configure a truststore.

Expected results:

- The connection succeeds with an otherwise untrusted self-signed certificate.
- The complete dynamic catalog lifecycle succeeds.

This mode is for testing and troubleshooting only.

### 3.5 Session Role

Configure Trino access control so that the JDBC user cannot run `CREATE CATALOG` or `DROP CATALOG` without a privileged system role.

First, omit `trino.jdbc.roles`.

Expected result: dynamic catalog creation fails with an authorization error.

Then configure:

```properties
trino.jdbc.roles=system:sysadmin
```

Expected results:

- The internal JDBC session activates the configured role.
- Dynamic catalog creation, query, and deletion succeed.

### 3.6 Arbitrary JDBC Driver Properties

Configure both a dedicated property and an overriding raw JDBC property:

```properties
trino.jdbc.ssl.enabled=true
trino.jdbc.ssl.verification=FULL
trino.jdbc.properties.SSLVerification=NONE
```

Expected result: the raw property overrides the value derived from the dedicated configuration, and the connection behaves as `SSLVerification=NONE`.

Then configure an invalid driver property:

```properties
trino.jdbc.properties.UnknownProperty=value
```

Expected results:

- Gravitino configuration parsing does not reject the property.
- The Trino JDBC driver reports the invalid property when establishing the connection.
- Logs may contain the property name but must not contain its value.

### 3.7 Default Port Derivation

Test these discovery URIs without explicit ports:

```properties
discovery.uri=http://trino-host
discovery.uri=https://trino-host
```

Expected internal JDBC targets:

```text
jdbc:trino://trino-host:80
jdbc:trino://trino-host:443
```

Where practical, place Trino or a reverse proxy on ports 80 and 443 and verify real connections, rather than relying only on log output.

### 3.8 Invalid Configuration Matrix

Each case must fail while the connector is starting. The error must identify the invalid property and must not expose secrets.

| Configuration | Expected result |
| --- | --- |
| TLS disabled with a truststore path | Fail because the truststore path requires TLS |
| TLS disabled with a truststore password | Fail because the truststore password requires TLS |
| TLS disabled with a truststore type | Fail because the truststore type requires TLS |
| TLS disabled with verification `CA` | Fail because `CA` requires TLS |
| TLS disabled with verification `NONE` | Fail because `NONE` requires TLS |
| Verification set to an unsupported value | Fail and list `FULL`, `CA`, and `NONE` as valid values |
| TLS enabled with a nonexistent truststore path | Fail and report that the file does not exist |
| Verification `NONE` with a truststore path | Fail because the combination is invalid |
| Truststore password is incorrect | Fail during JDBC TLS initialization |
| Truststore type is incorrect | Fail during JDBC TLS initialization |
| TLS connection points to an HTTP port | Fail and report that TLS is enabled |
| Plaintext connection points to an HTTPS port | Fail and report that TLS is disabled |
| `trino.jdbc.ssl.verification` is blank | Fall back to `FULL` |

### 3.9 Sensitive Information Protection

Configure unique marker values for the JDBC password, truststore password, and an extra JDBC property. Create and delete a dynamic catalog, then inspect:

- Trino server logs, including emitted `CREATE CATALOG` statements.
- Generated dynamic catalog property files.
- Gravitino Connector debug and error logs.

The following information must not appear in generated catalog configuration or logs:

- The value of `trino.jdbc.password`.
- The value of `trino.jdbc.ssl.truststore.password`.
- Values of `trino.jdbc.properties.*` entries.
- Other credentials used only by the internal JDBC connection.

## 4. Recommended Execution Order

1. Build the connector and prepare the HTTP and HTTPS environments.
2. Run HTTP backward-compatibility testing.
3. Run automatic TLS detection and explicit TLS testing.
4. Run the invalid configuration matrix.
5. Check sensitive-information protection.
6. Run `FULL`, `CA`, and `NONE` verification mode testing.
7. Run raw JDBC property override testing.
8. Run default port derivation testing.
9. Configure access control and run session-role testing.

## 5. Acceptance Criteria

- Existing HTTP deployments continue to work without new configuration.
- HTTPS is automatically enabled when `discovery.uri` uses the HTTPS scheme.
- Explicit TLS configuration supports valid truststores and all documented verification modes.
- Invalid TLS configuration fails early and never silently degrades to plaintext.
- Dynamic catalog creation, query, and deletion work through a valid TLS connection.
- A configured session role enables catalog operations that the JDBC user cannot otherwise perform.
- Raw JDBC properties reach the driver and override dedicated properties as documented.
- Discovery URIs without ports derive port 80 for HTTP and port 443 for HTTPS.
- Internal JDBC credentials are not copied into dynamic catalog statements, files, or logs.
- Failure messages identify the target JDBC endpoint and TLS state without exposing secrets.

## 6. Tasklist

### Preparation

- [ ] Build the Gravitino Trino Connector from the current branch.
- [x] Record the tested commit SHA and Trino version.
- [ ] Start the Gravitino server and confirm its health.
- [ ] Prepare a dynamic JDBC catalog and its backing database.
- [x] Prepare an HTTP Trino coordinator.
- [x] Prepare an HTTPS Trino coordinator with a self-signed or private-CA certificate.
- [x] Create a truststore that trusts the coordinator certificate.
- [ ] Create or obtain an untrusted truststore.
- [ ] Prepare invalid truststore path, password, and type inputs.
- [ ] Prepare Trino access-control rules and a privileged system role.

### Positive and Compatibility Tests

- [x] Verify HTTP backward compatibility and the complete catalog lifecycle.
- [x] Verify TLS is derived from an HTTPS `discovery.uri`.
- [x] Verify explicit `trino.jdbc.ssl.enabled=true` with `FULL` verification.
- [x] Verify `FULL` succeeds with a matching trusted certificate.
- [x] Verify `FULL` rejects a hostname mismatch.
- [x] Verify `CA` accepts a trusted certificate with a hostname mismatch.
- [x] Verify `NONE` accepts an untrusted self-signed certificate without a truststore.
- [ ] Verify a blank verification value falls back to `FULL`.
- [ ] Verify `trino.jdbc.roles=system:sysadmin` enables restricted catalog operations.
- [x] Verify a raw JDBC property overrides its dedicated configuration equivalent.
- [ ] Verify HTTP without an explicit port uses port 80.
- [ ] Verify HTTPS without an explicit port uses port 443.

### Negative Tests

- [x] Verify a truststore path is rejected when TLS is disabled.
- [ ] Verify a truststore password is rejected when TLS is disabled.
- [ ] Verify a truststore type is rejected when TLS is disabled.
- [ ] Verify `CA` is rejected when TLS is disabled.
- [ ] Verify `NONE` is rejected when TLS is disabled.
- [x] Verify an unsupported verification mode is rejected.
- [x] Verify a nonexistent truststore path is rejected.
- [x] Verify a truststore path is rejected with verification `NONE`.
- [ ] Verify an incorrect truststore password fails JDBC initialization.
- [ ] Verify an incorrect truststore type fails JDBC initialization.
- [ ] Verify TLS pointed at an HTTP port fails with a useful message.
- [ ] Verify plaintext pointed at an HTTPS port fails with a useful message.
- [ ] Verify an unknown raw JDBC property is reported by the JDBC driver.
- [ ] Verify catalog creation without the required session role is denied.

### Security and Evidence

- [x] Confirm JDBC passwords are absent from `CREATE CATALOG` statements.
- [x] Confirm truststore passwords are absent from logs and generated catalog files.
- [x] Confirm raw JDBC property values are absent from logs.
- [ ] Confirm only raw JDBC property names appear in debug logging.
- [x] Confirm connection errors include the JDBC endpoint and TLS state but no secrets.
- [ ] Save relevant configuration with secrets redacted.
- [ ] Save startup logs for each positive TLS mode.
- [ ] Save error logs for every negative configuration case.
- [ ] Save `SHOW CATALOGS` and query results for successful lifecycle tests.
- [ ] Record the result, observed behavior, and evidence location for every task.

### Completion

- [ ] Confirm all acceptance criteria are satisfied.
- [ ] Document any failure with reproduction steps and relevant redacted logs.
- [ ] Clean up the test catalogs, containers, certificates, and temporary configuration.

## 7. Execution Record

### Test Baseline

- Commit: `dfebbd70993911e0d2935c76099300b5da4b61ce`
- Trino version: `478`
- Java version: Temurin OpenJDK `17.0.20`
- Docker Engine version: `29.7.1`
- Test entry point: `trino-connector/integration-test/trino-test-tools/trino_integration_test.sh`
- Connector artifact: `distribution/gravitino-trino-connector-473-478`

### Branch Refresh on 2026-08-24

- Updated the branch from `dfebbd70993911e0d2935c76099300b5da4b61ce` to
  `4c9ce25f1` from Apache PR 12543.
- The update added mutual TLS keystore configuration and changed static Connector startup handling.
- PASS: Complete `trino-connector-473-478` module tests on the refreshed branch.
- PASS: Documentation build with `./gradlew :docs:build`.
- PASS: HTTP dynamic catalog create, query, and drop lifecycle on Trino 478.
- PASS: HTTPS automatic TLS detection with a PKCS12 truststore and complete catalog lifecycle.
- PASS: Invalid verification was propagated immediately from static Connector initialization with
  the valid verification modes in the error.

### Automated Pre-checks

- PASS: `TestGravitinoConfig` and `TestCatalogRegister` targeted unit tests.
- PASS: Complete `trino-connector-473-478` module build and unit tests.

### HTTP Backward Compatibility

- Status: PASS
- Trino configuration: `discovery.uri=http://0.0.0.0:8080`.
- Confirmed running Trino version `478`.
- Created dynamic catalog `gt_mysql_manual` through `gravitino.system.create_catalog`.
- Confirmed `gt_mysql_manual` appeared in `SHOW CATALOGS`.
- Confirmed schemas from the MySQL backend were visible, including `gt_db`.
- Dropped the catalog through `gravitino.system.drop_catalog`.
- Confirmed the dropped catalog no longer appeared in `SHOW CATALOGS`.

### HTTPS Automatic Detection

- Status: PASS
- Trino configuration: `discovery.uri=https://trino-ci-trino:8443` while HTTP port 8080 remained enabled for the test harness health check.
- The connector configuration did not set `trino.jdbc.ssl.enabled`.
- Used a self-signed coordinator certificate and a dedicated PKCS12 truststore.
- Confirmed the HTTPS `/v1/info` endpoint reported Trino `478` in the active state.
- Created dynamic catalog `gt_mysql_tls_auto` and confirmed it appeared in `SHOW CATALOGS`.
- Confirmed schemas from the MySQL backend were visible, including `gt_db`.
- Dropped the dynamic catalog successfully.
- Confirmed the truststore password marker did not appear in Trino or embedded-test logs.
- Confirmed the emitted dynamic `CREATE CATALOG` statement did not contain any internal `trino.jdbc.*` configuration.

### Explicit TLS and Verification Modes

- PASS: Explicit `trino.jdbc.ssl.enabled=true` with `FULL` verification and a matching certificate completed the catalog lifecycle.
- PASS: `CA` verification completed the catalog lifecycle while connecting to `127.0.0.1`, which was not present in the certificate SAN.
- PASS: `FULL` rejected the same endpoint with `SSLPeerUnverifiedException: Hostname 127.0.0.1 not verified`.
- PASS: `NONE`, without a truststore, completed the catalog lifecycle against the self-signed certificate.
- Observation: A certificate connection failure does not stop the Trino server. The connector retries its internal JDBC initialization in the background, and catalog synchronization remains unavailable until the connection succeeds.

### Invalid Configuration

- PASS: `NONE` with `trino.jdbc.ssl.truststore.path` was rejected as an invalid combination.
- PASS: A truststore path with TLS disabled was rejected because it requires `trino.jdbc.ssl.enabled=true`.
- PASS: Verification value `INVALID` was rejected with the valid set `[FULL, CA, NONE]`.
- PASS: A nonexistent truststore path was rejected with the complete missing path in the error.
- All observed configuration errors omitted password values.

### Raw JDBC Property Override

- PASS: Dedicated verification was set to `FULL`, while `trino.jdbc.properties.SSLVerification=NONE` was supplied.
- The connection succeeded without a truststore against the hostname-mismatched self-signed certificate.
- The complete dynamic catalog lifecycle succeeded, confirming that the raw JDBC property overrode the dedicated property.
