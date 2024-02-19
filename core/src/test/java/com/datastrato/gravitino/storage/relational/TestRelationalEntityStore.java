/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.meta.BaseMetalake;
import java.io.IOException;
import java.util.function.Function;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRelationalEntityStore {
  private static EntityStore store;

  @BeforeAll
  public static void setUp() {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_STORE)).thenReturn(Configs.RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_STORE))
        .thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    store = EntityStoreFactory.createEntityStore(config);
    store.initialize(config);
    Assertions.assertTrue(store instanceof RelationalEntityStore);
  }

  @AfterAll
  public static void teardown() throws IOException {
    store.close();
    store = null;
  }

  @Test
  public void testSetSerDe() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> store.setSerDe(null));
  }

  @Test
  public void testExecuteInTransaction() {
    Assertions.assertThrows(
        UnsupportedOperationException.class, () -> store.executeInTransaction(null));
  }

  @Test
  public void testExists() throws IOException {
    NameIdentifier nameIdentifier = Mockito.mock(NameIdentifier.class);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> store.exists(nameIdentifier, Entity.EntityType.METALAKE));
  }

  @Test
  public void testPut() throws IOException {
    BaseMetalake metalake = Mockito.mock(BaseMetalake.class);
    Assertions.assertThrows(UnsupportedOperationException.class, () -> store.put(metalake, false));
  }

  @Test
  public void testGet() {
    NameIdentifier nameIdentifier = Mockito.mock(NameIdentifier.class);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> store.get(nameIdentifier, Entity.EntityType.METALAKE, BaseMetalake.class));
  }

  @Test
  public void testUpdate() {
    NameIdentifier nameIdentifier = Mockito.mock(NameIdentifier.class);
    Function function = Mockito.mock(Function.class);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            store.update(nameIdentifier, BaseMetalake.class, Entity.EntityType.METALAKE, function));
  }

  @Test
  public void testList() {
    Namespace namespace = Mockito.mock(Namespace.class);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> store.list(namespace, BaseMetalake.class, Entity.EntityType.METALAKE));
  }

  @Test
  public void testDelete() {
    NameIdentifier nameIdentifier = Mockito.mock(NameIdentifier.class);
    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () -> store.delete(nameIdentifier, Entity.EntityType.METALAKE, false));
  }
}
