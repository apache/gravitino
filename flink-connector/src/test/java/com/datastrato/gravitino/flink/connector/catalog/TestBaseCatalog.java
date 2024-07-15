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
package com.datastrato.gravitino.flink.connector.catalog;

import com.apache.gravitino.SchemaChange;
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
