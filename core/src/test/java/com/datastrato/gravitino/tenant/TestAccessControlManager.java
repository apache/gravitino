/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.tenant;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.Group;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.TestEntityStore;
import com.datastrato.gravitino.User;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchUserException;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
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

  @Test
  public void testCreateGroup() {
    Map<String, String> props = ImmutableMap.of("key1", "value1");

    Group group =
        accessControlManager.createGroup("metalake", "create", Lists.newArrayList("user1"), props);
    Assertions.assertEquals("create", group.name());
    testProperties(props, group.properties());
    Assertions.assertTrue(group.users().contains("user1"));
    Assertions.assertEquals(1, group.users().size());

    // Test with GroupAlreadyExistsException
    Assertions.assertThrows(
        GroupAlreadyExistsException.class,
        () ->
            accessControlManager.createGroup(
                "metalake", "create", Lists.newArrayList("user1"), props));
  }

  @Test
  public void testLoadGroup() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createGroup("metalake", "loadGroup", Lists.newArrayList("user1"), props);

    Group group = accessControlManager.loadGroup("metalake", "loadGroup");
    Assertions.assertEquals("loadGroup", group.name());
    testProperties(props, group.properties());
    Assertions.assertTrue(group.users().contains("user1"));
    Assertions.assertEquals(1, group.users().size());

    // Test load non-existed group
    Throwable exception =
        Assertions.assertThrows(
            NoSuchGroupException.class,
            () -> accessControlManager.loadGroup("metalake", "not-exist"));
    Assertions.assertTrue(exception.getMessage().contains("Group not-exist does not exist"));
  }

  @Test
  public void testDropGroup() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createGroup("metalake", "testDrop", Lists.newArrayList("user1"), props);

    // Test drop group
    boolean dropped = accessControlManager.dropGroup("metalake", "testDrop");
    Assertions.assertTrue(dropped);

    // Test drop non-existed group
    boolean dropped1 = accessControlManager.dropGroup("metalake", "no-exist");
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
