/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.flink.connector.catalog;

import com.datastrato.gravitino.SchemaChange;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBaseCatalog {

  @Test
  public void testHiveSchemaChanges() {
    Map<String, String> currentProperties = ImmutableMap.of("key", "value", "key2", "value2");
    CatalogDatabase current = new CatalogDatabaseImpl(currentProperties, null);

    Map<String, String> newProperties = ImmutableMap.of("key2", "new-value2", "key3", "value3");
    CatalogDatabase updated = new CatalogDatabaseImpl(newProperties, null);

    SchemaChange[] schemaChange = BaseCatalog.getSchemaChange(current, updated);
    Assertions.assertEquals(3, schemaChange.length);
    Assertions.assertInstanceOf(SchemaChange.RemoveProperty.class, schemaChange[0]);
    Assertions.assertEquals("key", ((SchemaChange.RemoveProperty) schemaChange[0]).getProperty());

    Assertions.assertInstanceOf(SchemaChange.SetProperty.class, schemaChange[1]);
    Assertions.assertEquals("key3", ((SchemaChange.SetProperty) schemaChange[1]).getProperty());
    Assertions.assertEquals("value3", ((SchemaChange.SetProperty) schemaChange[1]).getValue());

    Assertions.assertInstanceOf(SchemaChange.SetProperty.class, schemaChange[2]);
    Assertions.assertEquals("key2", ((SchemaChange.SetProperty) schemaChange[2]).getProperty());
    Assertions.assertEquals("new-value2", ((SchemaChange.SetProperty) schemaChange[2]).getValue());
  }
}
