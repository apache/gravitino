/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.TestEntityStore;
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
      BaseMetalake.builder()
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

    User user = accessControlManager.addUser("metalake", "testCreate");
    Assertions.assertEquals("testCreate", user.name());
    Assertions.assertTrue(user.groups().isEmpty());
    Assertions.assertTrue(user.roles().isEmpty());

    user = accessControlManager.addUser("metalake", "testCreateWithOptionalField");

    Assertions.assertEquals("testCreateWithOptionalField", user.name());
    Assertions.assertTrue(user.groups().isEmpty());
    Assertions.assertTrue(user.roles().isEmpty());

    // Test with UserAlreadyExistsException
    Assertions.assertThrows(
        UserAlreadyExistsException.class,
        () -> accessControlManager.addUser("metalake", "testCreate"));
  }

  @Test
  public void testLoadUser() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.addUser("metalake", "testLoad");

    User user = accessControlManager.loadUser("metalake", "testLoad");
    Assertions.assertEquals("testLoad", user.name());

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

    accessControlManager.addUser("metalake", "testDrop");

    // Test drop user
    boolean dropped = accessControlManager.removeUser("metalake", "testDrop");
    Assertions.assertTrue(dropped);

    // Test drop non-existed user
    boolean dropped1 = accessControlManager.removeUser("metalake", "no-exist");
    Assertions.assertFalse(dropped1);
  }
}
