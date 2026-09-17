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
package org.apache.gravitino.idp;

import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS;
import static org.apache.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static org.apache.gravitino.Configs.ENTITY_STORE;
import static org.apache.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.apache.gravitino.Configs.STORE_DELETE_AFTER_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.gravitino.Config;
import org.apache.gravitino.exceptions.UnauthorizedException;
import org.apache.gravitino.idp.basic.IdpBasicConfigs;
import org.apache.gravitino.idp.basic.password.PasswordHasher;
import org.apache.gravitino.idp.basic.password.Sha3512PasswordHasher;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests verified Basic credential caching in {@link IdpUserGroupManager}. */
public class TestIdpUserGroupManagerCredentialCache {

  private static final String VALID_PASSWORD = "Passw0rd-1234";
  private static final String NEW_PASSWORD = "New-Password1!";
  private static final String WRONG_PASSWORD = "Wrong-Password1!";

  private Path h2Path;
  private IdpUserGroupManager manager;
  private CountingPasswordHasher passwordHasher;

  @BeforeEach
  public void setUp() throws Exception {
    h2Path = Files.createTempDirectory("gravitino_idp_cred_cache_h2_");
    passwordHasher = new CountingPasswordHasher(new Sha3512PasswordHasher());
    Config config = createH2Config(h2Path);
    config.set(IdpBasicConfigs.VERIFIED_CREDENTIAL_CACHE_ENABLED, true);
    config.set(IdpBasicConfigs.VERIFIED_CREDENTIAL_CACHE_EXPIRATION_SECS, 60L);
    config.set(IdpBasicConfigs.VERIFIED_CREDENTIAL_CACHE_MAX_SIZE, 1000L);
    manager =
        IdpUserGroupManagerTestHelper.newManager(
            config, RandomIdGenerator.INSTANCE, passwordHasher);
  }

  @AfterEach
  public void tearDown() throws IOException {
    if (manager != null) {
      manager.close();
      manager = null;
    }
    if (h2Path != null && Files.exists(h2Path)) {
      try (Stream<Path> paths = Files.walk(h2Path)) {
        paths
            .sorted(Comparator.reverseOrder())
            .forEach(TestIdpUserGroupManagerCredentialCache::deletePath);
      }
    }
  }

  @Test
  public void testSuccessfulAuthSkipsPasswordRehashOnCacheHit() throws Exception {
    manager.addUser("cacheHitUser", VALID_PASSWORD);
    passwordHasher.resetCounts();

    assertEquals("cacheHitUser", manager.authenticate("cacheHitUser", VALID_PASSWORD).name());
    assertEquals(1, passwordHasher.verifyCount.get());

    assertEquals("cacheHitUser", manager.authenticate("cacheHitUser", VALID_PASSWORD).name());
    assertEquals(1, passwordHasher.verifyCount.get());
  }

  @Test
  public void testFailedAuthAlwaysRunsPasswordVerify() throws Exception {
    manager.addUser("cacheMissUser", VALID_PASSWORD);
    passwordHasher.resetCounts();

    assertThrows(
        UnauthorizedException.class, () -> manager.authenticate("cacheMissUser", WRONG_PASSWORD));
    assertThrows(
        UnauthorizedException.class, () -> manager.authenticate("cacheMissUser", WRONG_PASSWORD));
    assertEquals(2, passwordHasher.verifyCount.get());
  }

  @Test
  public void testPasswordChangeInvalidatesCachedCredential() throws Exception {
    manager.addUser("cachePwdUser", VALID_PASSWORD);
    assertEquals("cachePwdUser", manager.authenticate("cachePwdUser", VALID_PASSWORD).name());
    passwordHasher.resetCounts();

    assertTrue(manager.changePassword("cachePwdUser", NEW_PASSWORD));
    assertThrows(
        UnauthorizedException.class, () -> manager.authenticate("cachePwdUser", VALID_PASSWORD));
    assertEquals(1, passwordHasher.verifyCount.get());

    assertEquals("cachePwdUser", manager.authenticate("cachePwdUser", NEW_PASSWORD).name());
    assertEquals(2, passwordHasher.verifyCount.get());
  }

  @Test
  public void testDisableInvalidatesCachedCredential() throws Exception {
    manager.addUser("cacheDisableUser", VALID_PASSWORD);
    assertEquals(
        "cacheDisableUser", manager.authenticate("cacheDisableUser", VALID_PASSWORD).name());

    assertTrue(manager.updateEnabled("cacheDisableUser", false));
    assertThrows(
        UnauthorizedException.class,
        () -> manager.authenticate("cacheDisableUser", VALID_PASSWORD));

    assertTrue(manager.updateEnabled("cacheDisableUser", true));
    passwordHasher.resetCounts();
    assertEquals(
        "cacheDisableUser", manager.authenticate("cacheDisableUser", VALID_PASSWORD).name());
    assertEquals(1, passwordHasher.verifyCount.get());
  }

  @Test
  public void testDisabledCacheRehashesEveryRequest() throws Exception {
    manager.close();
    manager = null;

    Path disabledCachePath = Files.createTempDirectory("gravitino_idp_cred_cache_disabled_h2_");
    Config config = createH2Config(disabledCachePath);
    config.set(IdpBasicConfigs.VERIFIED_CREDENTIAL_CACHE_ENABLED, false);
    CountingPasswordHasher hasher = new CountingPasswordHasher(new Sha3512PasswordHasher());
    try (IdpUserGroupManager noCacheManager =
        IdpUserGroupManagerTestHelper.newManager(config, RandomIdGenerator.INSTANCE, hasher)) {
      noCacheManager.addUser("noCacheUser", VALID_PASSWORD);
      hasher.resetCounts();
      noCacheManager.authenticate("noCacheUser", VALID_PASSWORD);
      noCacheManager.authenticate("noCacheUser", VALID_PASSWORD);
      assertEquals(2, hasher.verifyCount.get());
    } finally {
      if (Files.exists(disabledCachePath)) {
        try (Stream<Path> paths = Files.walk(disabledCachePath)) {
          paths
              .sorted(Comparator.reverseOrder())
              .forEach(TestIdpUserGroupManagerCredentialCache::deletePath);
        }
      }
    }
  }

  private static Config createH2Config(Path h2Path) {
    Config backendConfig = new Config(false) {};
    backendConfig.set(ENTITY_STORE, RELATIONAL_ENTITY_STORE);
    backendConfig.set(ENTITY_RELATIONAL_STORE, "h2");
    backendConfig.set(
        ENTITY_RELATIONAL_JDBC_BACKEND_URL,
        String.format("jdbc:h2:file:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", h2Path));
    backendConfig.set(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
    backendConfig.set(ENTITY_RELATIONAL_JDBC_BACKEND_MAX_CONNECTIONS, 100);
    backendConfig.set(ENTITY_RELATIONAL_JDBC_BACKEND_WAIT_MILLISECONDS, 1000L);
    backendConfig.set(STORE_DELETE_AFTER_TIME, 20 * 60 * 1000L);
    backendConfig.set(CACHE_ENABLED, false);
    return backendConfig;
  }

  private static void deletePath(Path path) {
    try {
      Files.deleteIfExists(path);
    } catch (IOException ignored) {
      // Best-effort cleanup for temporary H2 files.
    }
  }

  private static final class CountingPasswordHasher implements PasswordHasher {
    private final PasswordHasher delegate;
    private final AtomicInteger verifyCount = new AtomicInteger();

    private CountingPasswordHasher(PasswordHasher delegate) {
      this.delegate = delegate;
    }

    @Override
    public String hash(String plainPassword) {
      return delegate.hash(plainPassword);
    }

    @Override
    public boolean verify(String plainPassword, String hashedPassword) {
      verifyCount.incrementAndGet();
      return delegate.verify(plainPassword, hashedPassword);
    }

    private void resetCounts() {
      verifyCount.set(0);
    }
  }
}
