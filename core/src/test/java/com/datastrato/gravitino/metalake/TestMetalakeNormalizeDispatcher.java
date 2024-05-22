/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.metalake;

import static com.datastrato.gravitino.Entity.SYSTEM_METALAKE_RESERVED_NAME;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.MetalakeChange;
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
    NameIdentifier metalakeIdent1 = NameIdentifier.of(SYSTEM_METALAKE_RESERVED_NAME);
    Exception exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalakeNormalizeDispatcher.createMetalake(metalakeIdent1, null, null));
    Assertions.assertEquals("The metalake name 'system' is reserved.", exception.getMessage());

    NameIdentifier metalakeIdent2 = NameIdentifier.of("a-b");
    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> metalakeNormalizeDispatcher.createMetalake(metalakeIdent2, null, null));
    Assertions.assertEquals("The metalake name 'a-b' is illegal.", exception.getMessage());

    exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                metalakeNormalizeDispatcher.alterMetalake(
                    metalakeIdent2, MetalakeChange.rename("a-b")));
    Assertions.assertEquals("The metalake name 'a-b' is illegal.", exception.getMessage());
  }
}
