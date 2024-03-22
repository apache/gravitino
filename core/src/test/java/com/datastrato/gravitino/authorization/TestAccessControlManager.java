/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.StringIdentifier;
import com.datastrato.gravitino.TestEntityStore;
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

    User user =
        accessControlManager.createUser(
            "metalake",
            "testCreate",
            "first",
            "last",
            "display",
            "123@abc.com",
            true,
            null,
            null,
            props);
    Assertions.assertEquals("testCreate", user.name());
    testProperties(props, user.properties());
    Assertions.assertEquals("first", user.firstName());
    Assertions.assertEquals("last", user.lastName());
    Assertions.assertEquals("display", user.displayName());
    Assertions.assertTrue(user.active());
    Assertions.assertEquals(Lists.newArrayList("group"), user.groups());
    Assertions.assertEquals(Lists.newArrayList("role"), user.roles());
    Assertions.assertNull(user.defaultRole());
    Assertions.assertNull(user.comment());

    user =
        accessControlManager.createUser(
            "metalake",
            "testCreateWithOptionalField",
            "first",
            "last",
            "display",
            "123@abc.com",
            true,
            "role",
            "comment",
            props);

    Assertions.assertEquals("testCreateWithOptionalField", user.name());
    testProperties(props, user.properties());
    Assertions.assertEquals("first", user.firstName());
    Assertions.assertEquals("last", user.lastName());
    Assertions.assertEquals("display", user.displayName());
    Assertions.assertTrue(user.active());
    Assertions.assertEquals(Lists.newArrayList("group"), user.groups());
    Assertions.assertEquals(Lists.newArrayList("role"), user.roles());
    Assertions.assertEquals("role", user.defaultRole());
    Assertions.assertEquals("comment", user.comment());

    // Test with UserAlreadyExistsException
    Assertions.assertThrows(
        UserAlreadyExistsException.class,
        () ->
            accessControlManager.createUser(
                "metalake",
                "testCreate",
                "first",
                "last",
                "display",
                "123@abc.com",
                true,
                "role",
                "comment",
                props));
  }

  @Test
  public void testLoadUser() {
    Map<String, String> props = ImmutableMap.of("k1", "v1");

    accessControlManager.createUser(
        "metalake",
        "testLoad",
        "first",
        "last",
        "display",
        "123@abc.com",
        true,
        "role",
        "comment",
        props);

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

    accessControlManager.createUser(
        "metalake",
        "testDrop",
        "first",
        "last",
        "display",
        "123@abc.com",
        true,
        "role",
        "comment",
        props);

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
