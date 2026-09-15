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

package org.apache.gravitino.storage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

@Tag("gravitino-docker-test")
public class TestEntityStorageForLance extends AbstractEntityStorageTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testLanceTableCreateAndUpdate(String type, boolean enableCache)
      throws IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

      BaseMetalake metalake =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
      store.put(metalake, false);

      CatalogEntity catalogEntity =
          CatalogEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withName("catalog")
              .withNamespace(NamespaceUtil.ofCatalog("metalake"))
              .withType(Catalog.Type.RELATIONAL)
              .withProvider("generic-lakehouse")
              .withComment("This is a generic-lakehouse")
              .withProperties(ImmutableMap.of())
              .withAuditInfo(auditInfo)
              .build();

      store.put(catalogEntity, false);

      SchemaEntity schemaEntity =
          SchemaEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withName("schema")
              .withNamespace(NamespaceUtil.ofSchema("metalake", "catalog"))
              .withComment("This is a schema for generic-lakehouse")
              .withProperties(ImmutableMap.of())
              .withAuditInfo(auditInfo)
              .build();
      store.put(schemaEntity, false);

      long column1Id = RandomIdGenerator.INSTANCE.nextId();
      TableEntity table =
          TableEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withNamespace(NamespaceUtil.ofTable("metalake", "catalog", "schema"))
              .withName("table")
              .withAuditInfo(auditInfo)
              .withColumns(
                  Lists.newArrayList(
                      ColumnEntity.builder()
                          .withId(column1Id)
                          .withName("column1")
                          .withDataType(Types.StringType.get())
                          .withComment("test column")
                          .withPosition(1)
                          .withAuditInfo(auditInfo)
                          .build()))
              .withComment("This is a lance table")
              .withProperties(ImmutableMap.of("location", "/tmp/test", "format", "lance"))
              .build();
      store.put(table, false);
      TableEntity fetchedTable =
          store.get(table.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class);

      Assertions.assertEquals("/tmp/test", fetchedTable.properties().get("location"));
      Assertions.assertEquals("lance", fetchedTable.properties().get("format"));
      Assertions.assertEquals("This is a lance table", fetchedTable.comment());
      Assertions.assertEquals(1, fetchedTable.columns().size());
      Assertions.assertEquals("column1", fetchedTable.columns().get(0).name());

      TableEntity updatedTable =
          TableEntity.builder()
              .withId(table.id())
              .withNamespace(table.namespace())
              .withName(table.name())
              .withAuditInfo(auditInfo)
              .withColumns(
                  Lists.newArrayList(
                      ColumnEntity.builder()
                          .withId(column1Id)
                          .withName("column1")
                          .withDataType(Types.StringType.get())
                          .withComment("updated test column")
                          .withPosition(1)
                          .withAuditInfo(auditInfo)
                          .build(),
                      ColumnEntity.builder()
                          .withId(RandomIdGenerator.INSTANCE.nextId())
                          .withName("column2")
                          .withDataType(Types.IntegerType.get())
                          .withComment("new column")
                          .withPosition(2)
                          .withAuditInfo(auditInfo)
                          .build()))
              .withComment("This is an updated lance table")
              .withProperties(ImmutableMap.of("location", "/tmp/updated_test", "format", "lance"))
              .build();

      store.update(
          table.nameIdentifier(), TableEntity.class, Entity.EntityType.TABLE, e -> updatedTable);
      TableEntity fetchedUpdatedTable =
          store.get(table.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class);

      Assertions.assertEquals(
          "/tmp/updated_test", fetchedUpdatedTable.properties().get("location"));
      Assertions.assertEquals("lance", fetchedUpdatedTable.properties().get("format"));
      Assertions.assertEquals("This is an updated lance table", fetchedUpdatedTable.comment());
      Assertions.assertEquals(2, fetchedUpdatedTable.columns().size());
      for (ColumnEntity column : fetchedUpdatedTable.columns()) {
        if (column.name().equals("column1")) {
          Assertions.assertEquals("updated test column", column.comment());
        }
      }

      Assertions.assertTrue(
          fetchedUpdatedTable.columns().stream().anyMatch(c -> c.name().equals("column2")));

      Assertions.assertTrue(store.delete(table.nameIdentifier(), Entity.EntityType.TABLE));
      Assertions.assertFalse(store.exists(table.nameIdentifier(), Entity.EntityType.TABLE));

      destroy(type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
