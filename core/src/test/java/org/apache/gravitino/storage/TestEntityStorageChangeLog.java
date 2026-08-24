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

import java.time.Instant;
import java.util.List;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.storage.relational.mapper.EntityChangeLogMapper;
import org.apache.gravitino.storage.relational.po.cache.EntityChangeRecord;
import org.apache.gravitino.storage.relational.po.cache.OperateType;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/** Tests entity change logs through the relational {@link EntityStore}. */
@Tag("gravitino-docker-test")
public class TestEntityStorageChangeLog extends AbstractEntityStorageTest {
  private static final String METALAKE_NAME = "metalake_for_entity_store_change_log_test";
  private static final String CATALOG_NAME = "catalog_for_entity_store_change_log_test";
  private static final String SCHEMA_NAME = "schema_for_entity_store_change_log_test";

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testMutationChangeLogLifecycle(String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    init(type, config);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      BaseMetalake metalake =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, auditInfo);
      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog(METALAKE_NAME),
              CATALOG_NAME,
              auditInfo);
      SchemaEntity schema =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofSchema(METALAKE_NAME, CATALOG_NAME),
              SCHEMA_NAME,
              auditInfo);
      TableEntity table =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
              "table",
              auditInfo);

      long maxIdBeforeCreate = maxEntityChangeId();
      store.put(metalake, false);
      store.put(catalog, false);
      store.put(schema, false);
      store.put(table, false);
      Assertions.assertEquals(maxIdBeforeCreate, maxEntityChangeId());

      CatalogEntity updatedCatalog =
          CatalogEntity.builder()
              .withId(catalog.id())
              .withNamespace(catalog.namespace())
              .withName(catalog.name())
              .withType(catalog.getType())
              .withProvider(catalog.getProvider())
              .withComment("updated comment")
              .withProperties(catalog.getProperties())
              .withAuditInfo(auditInfo)
              .build();
      long maxIdBeforeAlter = maxEntityChangeId();
      store.update(
          catalog.nameIdentifier(),
          CatalogEntity.class,
          Entity.EntityType.CATALOG,
          entity -> updatedCatalog);
      assertEntityChange(
          maxIdBeforeAlter,
          Entity.EntityType.CATALOG,
          NameIdentifierUtil.ofCatalog(METALAKE_NAME, CATALOG_NAME).toString(),
          OperateType.ALTER);

      long maxIdBeforeOverwrite = maxEntityChangeId();
      store.put(table, true);
      assertEntityChange(
          maxIdBeforeOverwrite,
          Entity.EntityType.TABLE,
          NameIdentifierUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME, table.name())
              .toString(),
          OperateType.ALTER);

      long maxIdBeforeTableDrop = maxEntityChangeId();
      Assertions.assertTrue(store.delete(table.nameIdentifier(), Entity.EntityType.TABLE, false));
      assertEntityChange(
          maxIdBeforeTableDrop,
          Entity.EntityType.TABLE,
          table.nameIdentifier().toString(),
          OperateType.DROP);

      SchemaEntity renamedSchema =
          createSchemaEntity(schema.id(), schema.namespace(), SCHEMA_NAME + "_renamed", auditInfo);
      long maxIdBeforeRename = maxEntityChangeId();
      store.update(
          schema.nameIdentifier(),
          SchemaEntity.class,
          Entity.EntityType.SCHEMA,
          entity -> renamedSchema);
      assertEntityChange(
          maxIdBeforeRename,
          Entity.EntityType.SCHEMA,
          schema.nameIdentifier().toString(),
          OperateType.ALTER);

      long maxIdBeforeSchemaDrop = maxEntityChangeId();
      Assertions.assertTrue(
          store.delete(renamedSchema.nameIdentifier(), Entity.EntityType.SCHEMA, false));
      assertEntityChange(
          maxIdBeforeSchemaDrop,
          Entity.EntityType.SCHEMA,
          renamedSchema.nameIdentifier().toString(),
          OperateType.DROP);
      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testCascadeDropWritesOnlyRootChangeLog(String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    init(type, config);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      BaseMetalake metalake =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), METALAKE_NAME, auditInfo);
      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog(METALAKE_NAME),
              CATALOG_NAME,
              auditInfo);
      SchemaEntity schema =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofSchema(METALAKE_NAME, CATALOG_NAME),
              SCHEMA_NAME,
              auditInfo);
      TableEntity table =
          createTableEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofTable(METALAKE_NAME, CATALOG_NAME, SCHEMA_NAME),
              "table",
              auditInfo);

      store.put(metalake, false);
      store.put(catalog, false);
      store.put(schema, false);
      store.put(table, false);

      long maxIdBeforeDrop = maxEntityChangeId();
      Assertions.assertTrue(store.delete(schema.nameIdentifier(), Entity.EntityType.SCHEMA, true));
      assertEntityChange(
          maxIdBeforeDrop,
          Entity.EntityType.SCHEMA,
          schema.nameIdentifier().toString(),
          OperateType.DROP);
      Assertions.assertFalse(store.exists(table.nameIdentifier(), Entity.EntityType.TABLE));
      destroy(type);
    }
  }

  private static long maxEntityChangeId() {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, EntityChangeLogMapper::selectMaxChangeId);
  }

  private static List<EntityChangeRecord> listEntityChanges(long lastConsumedId) {
    return SessionUtils.doWithCommitAndFetchResult(
        EntityChangeLogMapper.class, mapper -> mapper.selectEntityChanges(lastConsumedId, 100));
  }

  private static void assertEntityChange(
      long lastConsumedId, Entity.EntityType entityType, String fullName, OperateType operateType) {
    List<EntityChangeRecord> entityChanges = listEntityChanges(lastConsumedId);
    Assertions.assertEquals(1, entityChanges.size());
    EntityChangeRecord entityChange = entityChanges.get(0);
    Assertions.assertEquals(METALAKE_NAME, entityChange.getMetalakeName());
    Assertions.assertEquals(entityType.name(), entityChange.getEntityType());
    Assertions.assertEquals(fullName, entityChange.getFullName());
    Assertions.assertEquals(operateType, entityChange.getOperateType());
  }
}
