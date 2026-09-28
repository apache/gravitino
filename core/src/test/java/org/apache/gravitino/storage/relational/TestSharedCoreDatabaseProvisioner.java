/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SharedCoreDatabaseProvisioner}'s pure logic: {@link
 * SharedCoreDatabaseProvisioner#isEnabled(String)}, {@link
 * SharedCoreDatabaseProvisioner#nextDatabaseName()}, and {@link
 * SharedCoreDatabaseProvisioner#buildJdbcUrlForDatabase(String, String)}. Provisioning against a
 * real server is exercised indirectly by the docker-tagged integration tests instead.
 */
public class TestSharedCoreDatabaseProvisioner {

  private static final String ADMIN_URL_PROPERTY =
      SharedCoreDatabaseProvisioner.ADMIN_PROPERTY_PREFIX + "mysql.adminUrl";

  // org.gradle.test.worker is set by Gradle itself for the lifetime of a forked test-worker JVM
  // and is also read elsewhere in the same JVM (CoreDatabaseShardTelemetryExtension), so tests
  // must save and restore it exactly as they found it rather than clearing it outright - Gradle
  // reuses worker JVMs across test classes, so clearing it here would corrupt telemetry (and,
  // for any future database-tagged class sharing this fork, database naming) for every class
  // that happens to run afterward in the same fork.
  private @Nullable String originalAdminUrlProperty;
  private @Nullable String originalWorkerProperty;

  @BeforeEach
  public void saveProperties() {
    originalAdminUrlProperty = System.getProperty(ADMIN_URL_PROPERTY);
    originalWorkerProperty = System.getProperty("org.gradle.test.worker");
  }

  @AfterEach
  public void restoreProperties() {
    restoreProperty(ADMIN_URL_PROPERTY, originalAdminUrlProperty);
    restoreProperty("org.gradle.test.worker", originalWorkerProperty);
  }

  private static void restoreProperty(String name, @Nullable String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }

  @Test
  public void testIsEnabledFalseWhenPropertyAbsent() {
    System.clearProperty(ADMIN_URL_PROPERTY);
    assertFalse(SharedCoreDatabaseProvisioner.isEnabled("mysql"));
  }

  @Test
  public void testIsEnabledFalseWhenPropertyEmpty() {
    System.setProperty(ADMIN_URL_PROPERTY, "");
    assertFalse(SharedCoreDatabaseProvisioner.isEnabled("mysql"));
  }

  @Test
  public void testIsEnabledTrueWhenPropertyPresent() {
    System.setProperty(ADMIN_URL_PROPERTY, "jdbc:mysql://127.0.0.1:3306");
    assertTrue(SharedCoreDatabaseProvisioner.isEnabled("mysql"));
  }

  @Test
  public void testNextDatabaseNameIsValidAndUnique() {
    System.setProperty("org.gradle.test.worker", "1");
    String first = SharedCoreDatabaseProvisioner.nextDatabaseName();
    String second = SharedCoreDatabaseProvisioner.nextDatabaseName();

    assertTrue(first.matches("^[a-zA-Z0-9_$]+$"), "name must match the allowed character set");
    assertTrue(first.length() <= 63, "name must not exceed the 63-character cap");
    assertTrue(first.startsWith("gravitino_core_1_"));
    assertNotEquals(first, second, "consecutive calls must never collide");
  }

  @Test
  public void testNextDatabaseNameSanitizesWorkerId() {
    // org.gradle.test.worker values are numeric in practice, but the sanitizer must still handle
    // a non-alphanumeric id defensively (e.g. a manually-set property) without producing an
    // invalid database name.
    System.setProperty("org.gradle.test.worker", "gradle-worker-1");
    String name = SharedCoreDatabaseProvisioner.nextDatabaseName();
    assertTrue(name.matches("^[a-zA-Z0-9_$]+$"));
  }

  @Test
  public void testBuildJdbcUrlForDatabaseMySqlNoPath() {
    String url =
        SharedCoreDatabaseProvisioner.buildJdbcUrlForDatabase(
            "jdbc:mysql://127.0.0.1:3306", "gravitino_core_1_1");
    assertEquals("jdbc:mysql://127.0.0.1:3306/gravitino_core_1_1", url);
  }

  @Test
  public void testBuildJdbcUrlForDatabasePostgresTrailingSlash() {
    String url =
        SharedCoreDatabaseProvisioner.buildJdbcUrlForDatabase(
            "jdbc:postgresql://127.0.0.1:5432/", "gravitino_core_1_1");
    assertEquals("jdbc:postgresql://127.0.0.1:5432/gravitino_core_1_1", url);
  }

  @Test
  public void testBuildJdbcUrlForDatabasePreservesQueryString() {
    String url =
        SharedCoreDatabaseProvisioner.buildJdbcUrlForDatabase(
            "jdbc:postgresql://127.0.0.1:5432/?sslmode=disable", "gravitino_core_1_1");
    assertEquals("jdbc:postgresql://127.0.0.1:5432/gravitino_core_1_1?sslmode=disable", url);
  }

  @Test
  public void testBuildJdbcUrlForDatabaseIpv6Host() {
    String url =
        SharedCoreDatabaseProvisioner.buildJdbcUrlForDatabase(
            "jdbc:mysql://[::1]:3306", "gravitino_core_1_1");
    assertEquals("jdbc:mysql://[::1]:3306/gravitino_core_1_1", url);
  }

  @Test
  public void testBuildJdbcUrlForDatabaseReplacesExistingDatabaseName() {
    String url =
        SharedCoreDatabaseProvisioner.buildJdbcUrlForDatabase(
            "jdbc:mysql://127.0.0.1:3306/some_other_db", "gravitino_core_1_1");
    assertEquals("jdbc:mysql://127.0.0.1:3306/gravitino_core_1_1", url);
  }

  @Test
  public void testBuildJdbcUrlForDatabaseHandlesSchemelessUrl() {
    // No "://" at all - schemeEnd < 0, so the whole string is treated as the host part.
    String url = SharedCoreDatabaseProvisioner.buildJdbcUrlForDatabase("127.0.0.1:3306", "db1");
    assertEquals("127.0.0.1:3306/db1", url);
  }

  @Test
  public void testNextDatabaseNameThrowsWhenWorkerIdMakesTheNameTooLong() {
    // MAX_SHARED_DB_NAME_LENGTH is 63 and there is no class-name segment to truncate here (unlike
    // BackendTestExtension's per-class variant), so an absurdly long worker id is the only way to
    // exceed the cap - confirm it fails loudly instead of silently truncating into a name another
    // worker could collide with.
    System.setProperty("org.gradle.test.worker", "w".repeat(60));
    assertThrows(IllegalStateException.class, SharedCoreDatabaseProvisioner::nextDatabaseName);
  }

  @Test
  public void testAdminPropertyPrefixMatchesTheContractCoreBuildGradleKtsPublishesTo() {
    // core/build.gradle.kts cannot import this Java test-source class at Gradle configuration
    // time, so it re-declares this same prefix as a Kotlin string literal instead. Pin the
    // literal here so a rename on either side that breaks the contract fails loudly (as a test
    // failure) instead of silently disabling shared-container mode (isEnabled() just returns
    // false when the properties never arrive, with no other signal).
    assertEquals("gravitino.test.db.", SharedCoreDatabaseProvisioner.ADMIN_PROPERTY_PREFIX);
    assertEquals(
        "gravitino.test.db.pool.maxConnections",
        SharedCoreDatabaseProvisioner.POOL_MAX_CONNECTIONS_PROPERTY);
  }
}
