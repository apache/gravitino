/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.memory.TestMemoryEntityStore;
import java.io.IOException;
import java.time.Instant;
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

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
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
  public void testAddUser() {

    User user = accessControlManager.addUser("metalake", "testAdd");
    Assertions.assertEquals("testAdd", user.name());
    Assertions.assertTrue(user.roles().isEmpty());

    user = accessControlManager.addUser("metalake", "testAddWithOptionalField");

    Assertions.assertEquals("testAddWithOptionalField", user.name());
    Assertions.assertTrue(user.roles().isEmpty());

    // Test with UserAlreadyExistsException
    Assertions.assertThrows(
        UserAlreadyExistsException.class,
        () -> accessControlManager.addUser("metalake", "testAdd"));
  }

  @Test
  public void testGetUser() {

    accessControlManager.addUser("metalake", "testGet");

    User user = accessControlManager.getUser("metalake", "testGet");
    Assertions.assertEquals("testGet", user.name());

    // Test to get non-existed user
    Throwable exception =
        Assertions.assertThrows(
            NoSuchUserException.class, () -> accessControlManager.getUser("metalake", "not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("User not-exist does not exist"));
  }

  @Test
  public void testRemoveUser() {

    accessControlManager.addUser("metalake", "testRemove");

    // Test to remove user
    boolean removed = accessControlManager.removeUser("metalake", "testRemove");
    Assertions.assertTrue(removed);

    // Test to remove non-existed user
    boolean removed1 = accessControlManager.removeUser("metalake", "no-exist");
    Assertions.assertFalse(removed1);
  }

  @Test
  public void testAddGroup() {

    Group group = accessControlManager.addGroup("metalake", "testAdd");
    Assertions.assertEquals("testAdd", group.name());
    Assertions.assertTrue(group.roles().isEmpty());

    group = accessControlManager.addGroup("metalake", "testAddWithOptionalField");

    Assertions.assertEquals("testAddWithOptionalField", group.name());
    Assertions.assertTrue(group.roles().isEmpty());

    // Test with GroupAlreadyExistsException
    Assertions.assertThrows(
        GroupAlreadyExistsException.class,
        () -> accessControlManager.addGroup("metalake", "testAdd"));
  }

  @Test
  public void testGetGroup() {

    accessControlManager.addGroup("metalake", "testGet");

    Group group = accessControlManager.getGroup("metalake", "testGet");
    Assertions.assertEquals("testGet", group.name());

    // Test to get non-existed group
    Throwable exception =
        Assertions.assertThrows(
            NoSuchGroupException.class,
            () -> accessControlManager.getGroup("metalake", "not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("Group not-exist does not exist"));
  }

  @Test
  public void testRemoveGroup() {

    accessControlManager.addGroup("metalake", "testRemove");

    // Test to remove group
    boolean removed = accessControlManager.removeGroup("metalake", "testRemove");
    Assertions.assertTrue(removed);

    // Test to remove non-existed group
    boolean removed1 = accessControlManager.removeUser("metalake", "no-exist");
    Assertions.assertFalse(removed1);
  }
}
