/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.TestEntityStore;
import com.datastrato.gravitino.User;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAccessControlManager {

  private static AccessControlManager accessControlManager;

  private static EntityStore entityStore;

  private static Config config;

  private static String metalake = "metalake";

  private static BaseMetalake metalakeEntity =
      new BaseMetalake.Builder()
          .withId(1L)
          .withName(metalake)
          .withAuditInfo(
              AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
          .withVersion(SchemaVersion.V_0_1)
          .build();

  @BeforeAll
  public static void setUp() throws Exception {
    config = new Config(false) {};

    entityStore = new TestEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    entityStore.put(metalakeEntity, true);

    accessControlManager = new AccessControlManager(entityStore, new RandomIdGenerator());
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testCreateUser() {
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    User user = accessControlManager.createUser("metalake", "user", props);
    Assertions.assertEquals("user", user.name());
    testProperties(props, user.properties());

    // Test with UserAlreadyExistsException
    Assertions.assertThrows(
        UserAlreadyExistsException.class,
        () -> accessControlManager.createUser("metalake", "user", props));
  }

  @Test
  public void testLoadUser() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createUser("metalake", "testLoad", props);

    User user = accessControlManager.loadUser("metalake", "testLoad");
    Assertions.assertEquals("testLoad", user.name());
    testProperties(props, user.properties());

    // Test load non-existed user
    Throwable exception =
        Assertions.assertThrows(
            NoSuchUserException.class,
            () -> accessControlManager.loadUser("metalake", "not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("User not-exist does not exist"));
  }

  @Test
  public void testDropUser() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createUser("metalake", "testDrop", props);

    // Test drop user
    boolean dropped = accessControlManager.dropUser("metalake", "testDrop");
    Assertions.assertTrue(dropped);

    // Test drop non-existed user
    boolean dropped1 = accessControlManager.dropUser("metalake", "no-exist");
    Assertions.assertFalse(dropped1);
  }

  private void testProperties(Map<String, String> expectedProps, Map<String, String> testProps) {
    expectedProps.forEach(
        (k, v) -> {
          Assertions.assertEquals(v, testProps.get(k));
        });

    Assertions.assertFalse(testProps.containsKey(StringIdentifier.ID_KEY));
  }
}
