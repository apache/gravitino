/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.metalake;

import static com.datastrato.gravitino.Entity.SYSTEM_METALAKE_RESERVED_NAME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.Metalake;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.memory.TestMemoryEntityStore;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestMetalakeNormalizeDispatcher {
  private static MetalakeNormalizeDispatcher metalakeNormalizeDispatcher;
  private static EntityStore entityStore;

  @BeforeAll
  public static void setUp() {
    Config config = new Config(false) {};

    entityStore = new TestMemoryEntityStore.InMemoryEntityStore();
    entityStore.initialize(config);
    entityStore.setSerDe(null);

    MetalakeManager metalakeManager = new MetalakeManager(entityStore, new RandomIdGenerator());
    metalakeNormalizeDispatcher = new MetalakeNormalizeDispatcher(metalakeManager);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    if (entityStore != null) {
      entityStore.close();
      entityStore = null;
    }
  }

  @Test
  public void testNameSpc() {
    // Test for valid names
    String[] legalNames = {"metalake", "_metalake", "1_metalake", "_", "1"};
    for (String legalName : legalNames) {
      NameIdentifier metalakeIdent = NameIdentifier.of(legalName);
      Metalake metalake = metalakeNormalizeDispatcher.createMetalake(metalakeIdent, null, null);
      Assertions.assertEquals(legalName, metalake.name());
    }

    // Test for illegal and reserved names
    NameIdentifier metalakeIdent1 = NameIdentifier.of(SYSTEM_METALAKE_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalakeNormalizeDispatcher.createMetalake(metalakeIdent1, null, null));
    Assertions.assertEquals("The metalake name 'system' is reserved.", exception.getMessage());

    String[] illegalNames = {
      "metalake-xxx",
      "metalake/xxx",
      "metalake.xxx",
      "metalake@xxx",
      "metalake#xxx",
      "metalake$xxx",
      "metalake%xxx",
      "metalake^xxx",
      "metalake&xxx",
      "metalake*xxx",
      "metalake+xxx",
      "metalake=xxx",
      "metalake|xxx",
      "metalake\\xxx",
      "metalake`xxx",
      "metalake~xxx",
      "metalake!xxx",
      "metalake\"xxx",
      "metalake'xxx",
      "metalake<xxx",
      "metalake>xxx",
      "metalake,xxx",
      "metalake?xxx",
      "metalake:xxx",
      "metalake;xxx",
      "metalake[xxx",
      "metalake]xxx",
      "metalake{xxx",
      "metalake}xxx"
    };
    for (String illegalName : illegalNames) {
      NameIdentifier metalakeIdent = NameIdentifier.of(illegalName);
      exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> metalakeNormalizeDispatcher.createMetalake(metalakeIdent, null, null));
      Assertions.assertEquals(
          "The metalake name '" + illegalName + "' is illegal.", exception.getMessage());
    }
  }
}
