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

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.EntityStoreFactory;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SupportsRelationOperations;
import org.apache.gravitino.authorization.AuthorizationUtils;
import org.apache.gravitino.authorization.Privilege.Condition;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.cache.CaffeineEntityCache;
import org.apache.gravitino.cache.EntityCacheKey;
import org.apache.gravitino.cache.EntityCacheRelationKey;
import org.apache.gravitino.cache.ReverseIndexCache;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionDefinitions;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionImpls;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionParams;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.FunctionEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.relational.RelationalEntityStore;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

@Tag("gravitino-docker-test")
public class TestEntityStorageRelationCache extends AbstractEntityStorageTest {

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testInvalidRelationCache(String type, boolean enableCache) throws Exception {
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

      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog",
              auditInfo);
      store.put(catalog, false);

      RoleEntity role =
          createRoleEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              AuthorizationUtils.ofRoleNamespace("metalake"),
              "role",
              auditInfo,
              "catalog");
      store.put(role, false);

      Role oldRole = store.get(role.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);

      CatalogEntity updatedCatalog =
          CatalogEntity.builder()
              .withId(catalog.id())
              .withNamespace(catalog.namespace())
              .withName("newCatalogName")
              .withAuditInfo(auditInfo)
              .withComment(catalog.getComment())
              .withProperties(catalog.getProperties())
              .withType(catalog.getType())
              .withProvider(catalog.getProvider())
              .build();
      store.update(
          catalog.nameIdentifier(),
          CatalogEntity.class,
          Entity.EntityType.CATALOG,
          e -> updatedCatalog);

      Role newRow = store.get(role.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      Assertions.assertNotEquals(oldRole, newRow);
      Assertions.assertNotEquals(oldRole.securableObjects(), newRow.securableObjects());
      List<SecurableObject> securableObjects = newRow.securableObjects();
      Assertions.assertEquals(1, securableObjects.size());
      Assertions.assertEquals("newCatalogName", securableObjects.get(0).name());

      SchemaEntity schema =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "newCatalogName"),
              "schema",
              auditInfo);

      store.put(schema, false);
      FilesetEntity fileset =
          createFilesetEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "newCatalogName", "schema"),
              "fileset",
              auditInfo);
      store.put(fileset, false);

      SecurableObject catalogObject =
          SecurableObjects.ofCatalog("newCatalogName", Lists.newArrayList());
      SecurableObject schemaObject =
          SecurableObjects.ofSchema(catalogObject, "schema", Lists.newArrayList());
      SecurableObject securableFileset01 =
          SecurableObjects.ofFileset(
              schemaObject, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));
      SecurableObject securableFileset02 =
          SecurableObjects.ofFileset(
              schemaObject, "fileset", Lists.newArrayList(Privileges.WriteFileset.allow()));

      RoleEntity readRole =
          RoleEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withName("roleReadFileset")
              .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
              .withProperties(null)
              .withAuditInfo(auditInfo)
              .withSecurableObjects(Lists.newArrayList(securableFileset01))
              .build();
      store.put(readRole, false);

      RoleEntity writeRole =
          RoleEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withName("roleWriteFileset")
              .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
              .withProperties(null)
              .withAuditInfo(auditInfo)
              .withSecurableObjects(Lists.newArrayList(securableFileset02))
              .build();
      store.put(writeRole, false);

      Role loadedReadRole =
          store.get(readRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      Role loadedWriteRole =
          store.get(writeRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      Assertions.assertEquals(1, loadedReadRole.securableObjects().size());
      Assertions.assertEquals(
          "newCatalogName.schema.fileset", loadedReadRole.securableObjects().get(0).fullName());
      Assertions.assertEquals(1, loadedWriteRole.securableObjects().size());
      Assertions.assertEquals(
          "newCatalogName.schema.fileset", loadedReadRole.securableObjects().get(0).fullName());
      Assertions.assertEquals(
          Condition.ALLOW,
          loadedReadRole.securableObjects().get(0).privileges().get(0).condition());
      Assertions.assertEquals(
          Condition.ALLOW,
          loadedWriteRole.securableObjects().get(0).privileges().get(0).condition());

      store.delete(readRole.nameIdentifier(), Entity.EntityType.ROLE);

      ReverseIndexCache reverseIndexCache =
          ((CaffeineEntityCache) ((RelationalEntityStore) store).getCache()).getReverseIndex();
      List<EntityCacheKey> reverseIndexValue =
          reverseIndexCache.get(
              NameIdentifier.of("metalake", "newCatalogName", "schema", "fileset"),
              Entity.EntityType.FILESET);
      Assertions.assertEquals(1, reverseIndexValue.size());
      Assertions.assertEquals(writeRole.nameIdentifier(), reverseIndexValue.get(0).identifier());

      store.put(readRole, true);
      store.get(readRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      reverseIndexValue =
          reverseIndexCache.get(
              NameIdentifier.of("metalake", "newCatalogName", "schema", "fileset"),
              Entity.EntityType.FILESET);
      Assertions.assertEquals(2, reverseIndexValue.size());
      List<NameIdentifier> ids =
          reverseIndexValue.stream().map(EntityCacheKey::identifier).collect(Collectors.toList());
      Assertions.assertTrue(ids.contains(readRole.nameIdentifier()));
      Assertions.assertTrue(ids.contains(writeRole.nameIdentifier()));

      store.delete(readRole.nameIdentifier(), Entity.EntityType.ROLE);
      store.delete(writeRole.nameIdentifier(), Entity.EntityType.ROLE);

      reverseIndexValue =
          reverseIndexCache.get(
              NameIdentifier.of("metalake", "newCatalogName", "schema", "fileset"),
              Entity.EntityType.FILESET);
      Assertions.assertNull(reverseIndexValue);

      store.put(readRole, true);
      store.put(writeRole, true);
      store.get(readRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      store.get(writeRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);

      store.update(
          fileset.nameIdentifier(),
          FilesetEntity.class,
          Entity.EntityType.FILESET,
          e ->
              createFilesetEntity(
                  fileset.id(), fileset.namespace(), "fileset_new", fileset.auditInfo()));

      loadedReadRole =
          store.get(readRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      loadedWriteRole =
          store.get(writeRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);

      Assertions.assertEquals(1, loadedReadRole.securableObjects().size());
      Assertions.assertEquals(
          "newCatalogName.schema.fileset_new", loadedReadRole.securableObjects().get(0).fullName());
      Assertions.assertEquals(1, loadedWriteRole.securableObjects().size());
      Assertions.assertEquals(
          "newCatalogName.schema.fileset_new", loadedReadRole.securableObjects().get(0).fullName());
      Assertions.assertEquals(
          Condition.ALLOW,
          loadedReadRole.securableObjects().get(0).privileges().get(0).condition());
      Assertions.assertEquals(
          Condition.ALLOW,
          loadedWriteRole.securableObjects().get(0).privileges().get(0).condition());

      store.update(
          NameIdentifier.of("metalake", "newCatalogName", "schema"),
          SchemaEntity.class,
          Entity.EntityType.SCHEMA,
          e ->
              createSchemaEntity(
                  schema.id(),
                  Namespace.of("metalake", "newCatalogName"),
                  "schema_new",
                  schema.auditInfo()));
      loadedReadRole =
          store.get(readRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      loadedWriteRole =
          store.get(writeRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      Assertions.assertEquals(1, loadedReadRole.securableObjects().size());
      Assertions.assertEquals(
          "newCatalogName.schema_new.fileset_new",
          loadedReadRole.securableObjects().get(0).fullName());
      Assertions.assertEquals(1, loadedWriteRole.securableObjects().size());
      Assertions.assertEquals(
          "newCatalogName.schema_new.fileset_new",
          loadedReadRole.securableObjects().get(0).fullName());

      UserEntity user1 =
          createUserEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              AuthorizationUtils.ofUserNamespace("metalake"),
              "user1",
              auditInfo);
      store.put(user1, false);

      store.delete(
          NameIdentifier.of("metalake", "newCatalogName", "schema_new", "fileset_new"),
          Entity.EntityType.FILESET);
      Assertions.assertFalse(store.exists(fileset.nameIdentifier(), Entity.EntityType.FILESET));

      loadedReadRole =
          store.get(readRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
      loadedWriteRole =
          store.get(writeRole.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);

      Assertions.assertEquals(0, loadedReadRole.securableObjects().size());
      Assertions.assertEquals(0, loadedWriteRole.securableObjects().size());

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testTagRelationCache(String type, boolean enableCache) throws Exception {
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

      Namespace namespace = NameIdentifierUtil.ofTag("metalake", "tag1").namespace();
      TagEntity tag1 =
          TagEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withNamespace(namespace)
              .withName("tag1")
              .withAuditInfo(auditInfo)
              .withProperties(Collections.emptyMap())
              .build();
      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog",
              auditInfo);

      store.put(catalog, false);
      store.put(tag1, false);

      SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
          catalog.nameIdentifier(),
          Entity.EntityType.CATALOG,
          new NameIdentifier[] {tag1.nameIdentifier()},
          new NameIdentifier[] {});

      List<TagEntity> tags =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              catalog.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, tags.size());
      Assertions.assertEquals(tag1, tags.get(0));

      RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
      CaffeineEntityCache caffeineEntityCache =
          (CaffeineEntityCache) relationalEntityStore.getCache();
      Cache<EntityCacheRelationKey, List<Entity>> cache = caffeineEntityCache.getCacheData();

      List<Entity> cachedTags =
          cache.get(
              EntityCacheRelationKey.of(
                  catalog.nameIdentifier(),
                  Entity.EntityType.CATALOG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);

      Assertions.assertNotNull(cachedTags);
      Assertions.assertEquals(1, cachedTags.size());
      Assertions.assertEquals(tag1, cachedTags.get(0));

      List<GenericEntity> genericEntities =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag1.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(1, genericEntities.size());
      Assertions.assertEquals(catalog.id(), genericEntities.get(0).id());
      Assertions.assertEquals(catalog.name(), genericEntities.get(0).name());

      CatalogEntity updatedCatalog =
          CatalogEntity.builder()
              .withId(catalog.id())
              .withNamespace(catalog.namespace())
              .withName("newCatalogName")
              .withAuditInfo(auditInfo)
              .withComment(catalog.getComment())
              .withProperties(catalog.getProperties())
              .withType(catalog.getType())
              .withProvider(catalog.getProvider())
              .build();
      store.update(
          catalog.nameIdentifier(),
          CatalogEntity.class,
          Entity.EntityType.CATALOG,
          e -> updatedCatalog);
      List<Entity> cachedTagsAfterCatalogUpdate =
          cache.get(
              EntityCacheRelationKey.of(
                  catalog.nameIdentifier(),
                  Entity.EntityType.CATALOG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNull(cachedTagsAfterCatalogUpdate);

      List<Entity> cachedTagsByTagAfterCatalogUpdate =
          cache.get(
              EntityCacheRelationKey.of(
                  tag1.nameIdentifier(),
                  Entity.EntityType.TAG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNull(cachedTagsByTagAfterCatalogUpdate);

      List<TagEntity> tagsAfterCatalogUpdate =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              updatedCatalog.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, tagsAfterCatalogUpdate.size());
      Assertions.assertEquals(tag1, tagsAfterCatalogUpdate.get(0));

      List<Entity> cachedTagsAfterReload =
          cache.get(
              EntityCacheRelationKey.of(
                  updatedCatalog.nameIdentifier(),
                  Entity.EntityType.CATALOG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNotNull(cachedTagsAfterReload);
      Assertions.assertEquals(1, cachedTagsAfterReload.size());
      Assertions.assertEquals(tag1, cachedTagsAfterReload.get(0));

      List<GenericEntity> genericEntitiesAfterCatalogUpdate =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag1.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(1, genericEntitiesAfterCatalogUpdate.size());
      Assertions.assertEquals(updatedCatalog.id(), genericEntitiesAfterCatalogUpdate.get(0).id());
      Assertions.assertEquals(
          updatedCatalog.name(), genericEntitiesAfterCatalogUpdate.get(0).name());

      List<Entity> cachedTagsByTagAfterReload =
          cache.get(
              EntityCacheRelationKey.of(
                  tag1.nameIdentifier(),
                  Entity.EntityType.TAG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNotNull(cachedTagsByTagAfterReload);
      Assertions.assertEquals(1, cachedTagsByTagAfterReload.size());
      Assertions.assertEquals(
          updatedCatalog.id(), ((GenericEntity) cachedTagsByTagAfterReload.get(0)).id());
      Assertions.assertEquals(
          updatedCatalog.name(), ((GenericEntity) cachedTagsByTagAfterReload.get(0)).name());

      TagEntity updatedTag1 =
          TagEntity.builder()
              .withId(tag1.id())
              .withNamespace(tag1.namespace())
              .withName("tagChanged")
              .withAuditInfo(auditInfo)
              .withProperties(tag1.properties())
              .build();
      store.update(tag1.nameIdentifier(), TagEntity.class, Entity.EntityType.TAG, e -> updatedTag1);

      List<Entity> cachedTagsAfterTagUpdate =
          cache.get(
              EntityCacheRelationKey.of(
                  updatedCatalog.nameIdentifier(),
                  Entity.EntityType.CATALOG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNull(cachedTagsAfterTagUpdate);

      List<Entity> cachedEntitiesAfterTagUpdate =
          cache.get(
              EntityCacheRelationKey.of(
                  tag1.nameIdentifier(),
                  Entity.EntityType.TAG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNull(cachedEntitiesAfterTagUpdate);

      List<TagEntity> tagsAfterTagUpdate =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              updatedCatalog.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, tagsAfterTagUpdate.size());
      Assertions.assertEquals(updatedTag1, tagsAfterTagUpdate.get(0));

      List<Entity> cachedTagsAfterTagReload =
          cache.get(
              EntityCacheRelationKey.of(
                  updatedCatalog.nameIdentifier(),
                  Entity.EntityType.CATALOG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNotNull(cachedTagsAfterTagReload);
      Assertions.assertEquals(1, cachedTagsAfterTagReload.size());
      Assertions.assertEquals(updatedTag1, cachedTagsAfterTagReload.get(0));

      List<GenericEntity> genericEntitiesAfterTagUpdate =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              updatedTag1.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(1, genericEntitiesAfterTagUpdate.size());
      Assertions.assertEquals(catalog.id(), genericEntitiesAfterTagUpdate.get(0).id());
      Assertions.assertEquals(updatedCatalog.name(), genericEntitiesAfterTagUpdate.get(0).name());
      List<Entity> cachedTagsByTagAfterTagReload =
          cache.get(
              EntityCacheRelationKey.of(
                  updatedTag1.nameIdentifier(),
                  Entity.EntityType.TAG,
                  SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL),
              k -> null);
      Assertions.assertNotNull(cachedTagsByTagAfterTagReload);
      Assertions.assertEquals(1, cachedTagsByTagAfterTagReload.size());
      Assertions.assertEquals(
          updatedCatalog.id(), ((GenericEntity) cachedTagsByTagAfterTagReload.get(0)).id());
      Assertions.assertEquals(
          updatedCatalog.name(), ((GenericEntity) cachedTagsByTagAfterTagReload.get(0)).name());
      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testTagRelationMultipleBindings(String type, boolean enableCache) throws Exception {
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

      Namespace tagNamespace = NameIdentifierUtil.ofTag("metalake", "tag1").namespace();
      TagEntity tag1 =
          TagEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withNamespace(tagNamespace)
              .withName("tag1")
              .withAuditInfo(auditInfo)
              .withProperties(Collections.emptyMap())
              .build();

      CatalogEntity catalog1 =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog1",
              auditInfo);

      store.put(tag1, false);
      store.put(catalog1, false);

      SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
          catalog1.nameIdentifier(),
          Entity.EntityType.CATALOG,
          new NameIdentifier[] {tag1.nameIdentifier()},
          new NameIdentifier[] {});

      List<TagEntity> tagsForCatalog1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              catalog1.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, tagsForCatalog1.size());
      Assertions.assertEquals(tag1, tagsForCatalog1.get(0));

      List<GenericEntity> entitiesForTag1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag1.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(1, entitiesForTag1.size());
      Assertions.assertEquals(catalog1.id(), entitiesForTag1.get(0).id());
      Assertions.assertEquals(catalog1.name(), entitiesForTag1.get(0).name());

      CatalogEntity catalog2 =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog2",
              auditInfo);
      store.put(catalog2, false);

      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
          catalog2.nameIdentifier(),
          Entity.EntityType.CATALOG,
          new NameIdentifier[] {tag1.nameIdentifier()},
          new NameIdentifier[] {});

      tagsForCatalog1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              catalog1.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, tagsForCatalog1.size());
      Assertions.assertEquals(tag1, tagsForCatalog1.get(0));

      List<TagEntity> tagsForCatalog2 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              catalog2.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, tagsForCatalog2.size());
      Assertions.assertEquals(tag1, tagsForCatalog2.get(0));

      entitiesForTag1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag1.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(2, entitiesForTag1.size());
      List<Long> entityIds =
          entitiesForTag1.stream().map(GenericEntity::id).collect(Collectors.toList());
      List<String> entityNames =
          entitiesForTag1.stream().map(GenericEntity::name).collect(Collectors.toList());
      Assertions.assertTrue(entityIds.contains(catalog1.id()));
      Assertions.assertTrue(entityIds.contains(catalog2.id()));
      Assertions.assertTrue(entityNames.contains(catalog1.name()));
      Assertions.assertTrue(entityNames.contains(catalog2.name()));
      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testPolicyRelationMultipleBindings(String type, boolean enableCache) throws Exception {
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

      Namespace policyNamespace = NameIdentifierUtil.ofPolicy("metalake", "policy1").namespace();
      PolicyEntity policy1 =
          PolicyEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withNamespace(policyNamespace)
              .withName("policy1")
              .withPolicyType(Policy.BuiltInType.CUSTOM)
              .withContent(
                  PolicyContents.custom(
                      ImmutableMap.of("rule", "allow-all"),
                      Collections.singleton(MetadataObject.Type.CATALOG),
                      Collections.emptyMap()))
              .withAuditInfo(auditInfo)
              .build();

      CatalogEntity catalog1 =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog1",
              auditInfo);

      store.put(policy1, false);
      store.put(catalog1, false);

      SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
          catalog1.nameIdentifier(),
          Entity.EntityType.CATALOG,
          new NameIdentifier[] {policy1.nameIdentifier()},
          new NameIdentifier[] {});

      List<PolicyEntity> policiesForCatalog1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
              catalog1.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, policiesForCatalog1.size());
      Assertions.assertEquals(policy1, policiesForCatalog1.get(0));

      List<GenericEntity> entitiesForPolicy1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
              policy1.nameIdentifier(),
              Entity.EntityType.POLICY,
              true);
      Assertions.assertEquals(1, entitiesForPolicy1.size());
      Assertions.assertEquals(catalog1.id(), entitiesForPolicy1.get(0).id());

      CatalogEntity catalog2 =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog2",
              auditInfo);
      store.put(catalog2, false);

      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
          catalog2.nameIdentifier(),
          Entity.EntityType.CATALOG,
          new NameIdentifier[] {policy1.nameIdentifier()},
          new NameIdentifier[] {});

      policiesForCatalog1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
              catalog1.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, policiesForCatalog1.size());
      Assertions.assertEquals(policy1, policiesForCatalog1.get(0));

      List<PolicyEntity> policiesForCatalog2 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
              catalog2.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, policiesForCatalog2.size());
      Assertions.assertEquals(policy1, policiesForCatalog2.get(0));

      entitiesForPolicy1 =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.POLICY_METADATA_OBJECT_REL,
              policy1.nameIdentifier(),
              Entity.EntityType.POLICY,
              true);
      Assertions.assertEquals(2, entitiesForPolicy1.size());
      List<Long> entityIds =
          entitiesForPolicy1.stream().map(GenericEntity::id).collect(Collectors.toList());
      Assertions.assertTrue(entityIds.contains(catalog1.id()));
      Assertions.assertTrue(entityIds.contains(catalog2.id()));
      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testFunctionTagRelationCacheInvalidation(String type, boolean enableCache) throws Exception {
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

      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog",
              auditInfo);
      store.put(catalog, false);

      SchemaEntity schema =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog"),
              "schema",
              auditInfo);
      store.put(schema, false);

      TagEntity tag =
          TagEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withNamespace(NameIdentifierUtil.ofTag("metalake", "tag1").namespace())
              .withName("tag1")
              .withAuditInfo(auditInfo)
              .withProperties(Collections.emptyMap())
              .build();
      store.put(tag, false);

      FunctionEntity function =
          createFunctionEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog", "schema"),
              "function_old",
              auditInfo);
      store.put(function, false);

      SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;
      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
          function.nameIdentifier(),
          Entity.EntityType.FUNCTION,
          new NameIdentifier[] {tag.nameIdentifier()},
          new NameIdentifier[] {});

      List<TagEntity> tags =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              function.nameIdentifier(),
              Entity.EntityType.FUNCTION,
              true);
      Assertions.assertEquals(1, tags.size());
      Assertions.assertEquals(tag.name(), tags.get(0).name());

      List<GenericEntity> taggedObjects =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(1, taggedObjects.size());
      Assertions.assertEquals(function.id(), taggedObjects.get(0).id());
      Assertions.assertEquals("catalog.schema.function_old", taggedObjects.get(0).name());

      FunctionEntity renamedFunction =
          createFunctionEntity(
              function.id(),
              Namespace.of("metalake", "catalog", "schema"),
              "function_new",
              auditInfo);
      store.update(
          function.nameIdentifier(),
          FunctionEntity.class,
          Entity.EntityType.FUNCTION,
          e -> renamedFunction);

      if (enableCache && store instanceof RelationalEntityStore) {
        RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
        if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
          CaffeineEntityCache cache = (CaffeineEntityCache) relationalEntityStore.getCache();
          ReverseIndexCache reverseIndexCache = cache.getReverseIndex();
          Assertions.assertNull(
              reverseIndexCache.get(function.nameIdentifier(), Entity.EntityType.FUNCTION));
        }
      }

      List<TagEntity> tagsAfterRename =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              renamedFunction.nameIdentifier(),
              Entity.EntityType.FUNCTION,
              true);
      Assertions.assertEquals(1, tagsAfterRename.size());
      Assertions.assertEquals(tag.name(), tagsAfterRename.get(0).name());

      List<GenericEntity> taggedObjectsAfterRename =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(1, taggedObjectsAfterRename.size());
      Assertions.assertEquals(renamedFunction.id(), taggedObjectsAfterRename.get(0).id());
      Assertions.assertEquals(
          "catalog.schema.function_new", taggedObjectsAfterRename.get(0).name());

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testViewNotIndexedInReverseCache(String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);

      // Create parent entities required by relational store
      BaseMetalake metalake =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
      store.put(metalake, false);

      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog",
              auditInfo);
      store.put(catalog, false);

      SchemaEntity schema =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog"),
              "schema",
              auditInfo);
      store.put(schema, false);

      Namespace viewNamespace = Namespace.of("metalake", "catalog", "schema");
      ViewEntity view =
          ViewEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withName("test_view")
              .withNamespace(viewNamespace)
              .withColumns(new Column[0])
              .withRepresentations(
                  new Representation[] {
                    SQLRepresentation.builder().withDialect("unknown").withSql("SELECT 1").build()
                  })
              .withAuditInfo(auditInfo)
              .build();

      // Use store.put() to trigger the actual cache + reverse index flow
      store.put(view, false);

      // Verify view IS in forward cache (performance cache for get operations)
      ViewEntity retrievedView =
          store.get(view.nameIdentifier(), Entity.EntityType.VIEW, ViewEntity.class);
      Assertions.assertNotNull(retrievedView);
      Assertions.assertEquals(view.id(), retrievedView.id());
      Assertions.assertEquals(view.name(), retrievedView.name());

      // Get reverse index cache to verify view is NOT indexed
      ReverseIndexCache reverseIndexCache =
          ((CaffeineEntityCache) ((RelationalEntityStore) store).getCache()).getReverseIndex();

      // Verify view is NOT in reverse index cache
      // Views have namespace and should be skipped by GENERIC_METADATA_OBJECT_REVERSE_RULE
      List<EntityCacheKey> reverseIndexValue =
          reverseIndexCache.get(view.nameIdentifier(), Entity.EntityType.VIEW);
      Assertions.assertNull(
          reverseIndexValue,
          "Views should NOT be indexed in reverse cache - they have namespace and don't support tags/policies");

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testCacheInvalidationOnNewRelation(String type, boolean enableCache) throws Exception {
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

      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog",
              auditInfo);
      store.put(catalog, false);

      SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

      // 1. Fetch relation, it should be empty
      List<TagEntity> tags =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              catalog.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertTrue(tags.isEmpty());

      // 2. Verify cache has empty list if cache is enabled
      if (enableCache && store instanceof RelationalEntityStore) {
        RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
        if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
          CaffeineEntityCache cache = (CaffeineEntityCache) relationalEntityStore.getCache();
          List<Entity> cachedEntities =
              cache
                  .getCacheData()
                  .getIfPresent(
                      EntityCacheRelationKey.of(
                          catalog.nameIdentifier(),
                          Entity.EntityType.CATALOG,
                          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL));
          Assertions.assertNotNull(cachedEntities);
          Assertions.assertTrue(cachedEntities.isEmpty());
        }
      }

      // 3. Create a tag and add relation
      Namespace tagNamespace = NameIdentifierUtil.ofTag("metalake", "tag1").namespace();
      TagEntity tag1 =
          TagEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withNamespace(tagNamespace)
              .withName("tag1")
              .withAuditInfo(auditInfo)
              .withProperties(Collections.emptyMap())
              .build();
      store.put(tag1, false);

      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
          catalog.nameIdentifier(),
          Entity.EntityType.CATALOG,
          new NameIdentifier[] {tag1.nameIdentifier()},
          new NameIdentifier[] {});

      // 4. Fetch relation again, it should not be empty
      tags =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              catalog.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, tags.size());
      Assertions.assertEquals(tag1.name(), tags.get(0).name());

      List<UserEntity> owners =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.OWNER_REL,
              catalog.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertTrue(owners.isEmpty());

      if (enableCache && store instanceof RelationalEntityStore) {
        RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
        if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
          CaffeineEntityCache cache = (CaffeineEntityCache) relationalEntityStore.getCache();
          List<Entity> cachedOwners =
              cache
                  .getCacheData()
                  .getIfPresent(
                      EntityCacheRelationKey.of(
                          catalog.nameIdentifier(),
                          Entity.EntityType.CATALOG,
                          SupportsRelationOperations.Type.OWNER_REL));
          Assertions.assertNotNull(cachedOwners);
          Assertions.assertTrue(cachedOwners.isEmpty());
        }
      }

      UserEntity ownerUser =
          createUserEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              AuthorizationUtils.ofUserNamespace("metalake"),
              "ownerUser",
              auditInfo);
      store.put(ownerUser, false);

      relationOperations.insertRelation(
          SupportsRelationOperations.Type.OWNER_REL,
          catalog.nameIdentifier(),
          Entity.EntityType.CATALOG,
          ownerUser.nameIdentifier(),
          Entity.EntityType.USER,
          true);

      owners =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.OWNER_REL,
              catalog.nameIdentifier(),
              Entity.EntityType.CATALOG,
              true);
      Assertions.assertEquals(1, owners.size());
      Assertions.assertEquals(ownerUser.name(), owners.get(0).name());

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testCacheInvalidationOnNewRelationReverse(String type, boolean enableCache)
      throws Exception {
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

      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(),
              NamespaceUtil.ofCatalog("metalake"),
              "catalog",
              auditInfo);
      store.put(catalog, false);

      Namespace tagNamespace = NameIdentifierUtil.ofTag("metalake", "tag1").namespace();
      TagEntity tag1 =
          TagEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withNamespace(tagNamespace)
              .withName("tag1")
              .withAuditInfo(auditInfo)
              .withProperties(Collections.emptyMap())
              .build();
      store.put(tag1, false);

      SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

      // 1. Fetch relation for Tag (Target side), it should be empty
      // This populates the cache for (Tag, TAG, REL) with empty list
      List<GenericEntity> entities =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag1.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertTrue(entities.isEmpty());

      // 2. Verify cache has empty list if cache is enabled
      if (enableCache && store instanceof RelationalEntityStore) {
        RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
        if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
          CaffeineEntityCache cache = (CaffeineEntityCache) relationalEntityStore.getCache();
          List<Entity> cachedEntities =
              cache
                  .getCacheData()
                  .getIfPresent(
                      EntityCacheRelationKey.of(
                          tag1.nameIdentifier(),
                          Entity.EntityType.TAG,
                          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL));
          Assertions.assertNotNull(cachedEntities);
          Assertions.assertTrue(cachedEntities.isEmpty());
        }
      }

      // 3. Add relation from Catalog (Source side)
      relationOperations.updateEntityRelations(
          SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
          catalog.nameIdentifier(),
          Entity.EntityType.CATALOG,
          new NameIdentifier[] {tag1.nameIdentifier()},
          new NameIdentifier[] {});

      // 4. Fetch relation for Tag again, it should NOT be empty
      entities =
          relationOperations.listEntitiesByRelation(
              SupportsRelationOperations.Type.TAG_METADATA_OBJECT_REL,
              tag1.nameIdentifier(),
              Entity.EntityType.TAG,
              true);
      Assertions.assertEquals(1, entities.size());
      Assertions.assertEquals(catalog.name(), entities.get(0).name());

      destroy(type);
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRoleCacheEvictedOnFunctionDeletion(String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "schema",
                auditInfo);
        store.put(schema, false);

        FunctionEntity function =
            createFunctionEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog", "schema"),
                "function1",
                auditInfo);
        store.put(function, false);

        SecurableObject catalogObject =
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
        SecurableObject schemaObject =
            SecurableObjects.ofSchema(
                catalogObject, "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
        SecurableObject functionObject =
            SecurableObjects.ofFunction(
                schemaObject, "function1", Lists.newArrayList(Privileges.ExecuteFunction.allow()));

        RoleEntity role =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("functionRole")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(
                    Lists.newArrayList(catalogObject, schemaObject, functionObject))
                .build();
        store.put(role, false);

        // Read role to populate cache; ROLE_SECURABLE_OBJECT_REVERSE_RULE writes:
        // funcIdent:FUNCTION -> [roleIdent:ROLE] into the reverse index.
        RoleEntity cachedRole =
            store.get(role.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
        Assertions.assertEquals(3, cachedRole.securableObjects().size());

        if (enableCache && store instanceof RelationalEntityStore) {
          RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
          if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
            ReverseIndexCache reverseIndex =
                ((CaffeineEntityCache) relationalEntityStore.getCache()).getReverseIndex();
            List<EntityCacheKey> reverseKeys =
                reverseIndex.get(function.nameIdentifier(), Entity.EntityType.FUNCTION);
            Assertions.assertNotNull(reverseKeys);
            Assertions.assertTrue(
                reverseKeys.stream().anyMatch(k -> k.identifier().equals(role.nameIdentifier())));
          }
        }

        // Delete the function; BFS invalidation should evict the role from cache via reverse index.
        store.delete(function.nameIdentifier(), Entity.EntityType.FUNCTION, false);

        if (enableCache && store instanceof RelationalEntityStore) {
          RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
          if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
            CaffeineEntityCache cache = (CaffeineEntityCache) relationalEntityStore.getCache();
            Assertions.assertNull(
                cache
                    .getCacheData()
                    .getIfPresent(
                        EntityCacheRelationKey.of(role.nameIdentifier(), Entity.EntityType.ROLE)));
          }
        }

        // Re-read role; the function securable object should have been removed from DB via cascade.
        RoleEntity reloadedRole =
            store.get(role.nameIdentifier(), Entity.EntityType.ROLE, RoleEntity.class);
        long functionSecurableObjects =
            reloadedRole.securableObjects().stream()
                .filter(so -> so.type() == MetadataObject.Type.FUNCTION)
                .count();
        Assertions.assertEquals(0, functionSecurableObjects);

      } finally {
        destroy(type);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("storageProvider")
  void testFunctionOwnerRelationCacheInvalidation(String type, boolean enableCache)
      throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "schema",
                auditInfo);
        store.put(schema, false);

        FunctionEntity function =
            createFunctionEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog", "schema"),
                "function1",
                auditInfo);
        store.put(function, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // 1. Query owner of function - should be empty initially and result is cached
        List<UserEntity> owners =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.OWNER_REL,
                function.nameIdentifier(),
                Entity.EntityType.FUNCTION,
                true);
        Assertions.assertTrue(owners.isEmpty());

        if (enableCache && store instanceof RelationalEntityStore) {
          RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
          if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
            CaffeineEntityCache cache = (CaffeineEntityCache) relationalEntityStore.getCache();
            List<Entity> cachedOwners =
                cache
                    .getCacheData()
                    .getIfPresent(
                        EntityCacheRelationKey.of(
                            function.nameIdentifier(),
                            Entity.EntityType.FUNCTION,
                            SupportsRelationOperations.Type.OWNER_REL));
            Assertions.assertNotNull(cachedOwners);
            Assertions.assertTrue(cachedOwners.isEmpty());
          }
        }

        // 2. Set an owner; cache for OWNER_REL should be invalidated
        UserEntity ownerUser =
            createUserEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                AuthorizationUtils.ofUserNamespace("metalake"),
                "ownerUser",
                auditInfo);
        store.put(ownerUser, false);

        relationOperations.insertRelation(
            SupportsRelationOperations.Type.OWNER_REL,
            function.nameIdentifier(),
            Entity.EntityType.FUNCTION,
            ownerUser.nameIdentifier(),
            Entity.EntityType.USER,
            true);

        // 3. Query owner again - should return the owner and be re-cached
        owners =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.OWNER_REL,
                function.nameIdentifier(),
                Entity.EntityType.FUNCTION,
                true);
        Assertions.assertEquals(1, owners.size());
        Assertions.assertEquals(ownerUser.name(), owners.get(0).name());

        // 4. Rename the function; old OWNER_REL cache entry should be evicted via BFS invalidation
        FunctionEntity renamedFunction =
            createFunctionEntity(
                function.id(),
                Namespace.of("metalake", "catalog", "schema"),
                "function_renamed",
                auditInfo);
        store.update(
            function.nameIdentifier(),
            FunctionEntity.class,
            Entity.EntityType.FUNCTION,
            e -> renamedFunction);

        if (enableCache && store instanceof RelationalEntityStore) {
          RelationalEntityStore relationalEntityStore = (RelationalEntityStore) store;
          if (relationalEntityStore.getCache() instanceof CaffeineEntityCache) {
            CaffeineEntityCache cache = (CaffeineEntityCache) relationalEntityStore.getCache();
            // The old function's OWNER_REL cache should have been evicted by BFS invalidation
            List<Entity> cachedOwners =
                cache
                    .getCacheData()
                    .getIfPresent(
                        EntityCacheRelationKey.of(
                            function.nameIdentifier(),
                            Entity.EntityType.FUNCTION,
                            SupportsRelationOperations.Type.OWNER_REL));
            Assertions.assertNull(cachedOwners);
          }
        }

        // 5. Query owner via new name - should still return the correct owner
        owners =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.OWNER_REL,
                renamedFunction.nameIdentifier(),
                Entity.EntityType.FUNCTION,
                true);
        Assertions.assertEquals(1, owners.size());
        Assertions.assertEquals(ownerUser.name(), owners.get(0).name());

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Reproduces GitHub issue #11297: after {@code revokePrivilegesFromRole} removes the last
   * privilege from a role's securable schema, {@code
   * listEntitiesByRelation(METADATA_OBJECT_ROLE_REL)} must not return the stale role from cache.
   *
   * <p>Flow: create schema + role → list binding roles (populates cache) → update role to remove
   * securable object (simulates revoke) → list binding roles again → must reflect the change.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRevokePrivilegeInvalidatesMetadataObjectRoleRelCache(String type, boolean enableCache)
      throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "test_schema",
                auditInfo);
        store.put(schema, false);

        SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
        SecurableObject schemaObject =
            SecurableObjects.ofSchema(
                catalogObject, "test_schema", Lists.newArrayList(Privileges.UseSchema.allow()));

        RoleEntity role =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("test_role")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList(schemaObject))
                .build();
        store.put(role, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm up the METADATA_OBJECT_ROLE_REL cache (simulates listBindingRoleNames after grant)
        List<RoleEntity> rolesBeforeRevoke =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertEquals(1, rolesBeforeRevoke.size());
        Assertions.assertEquals("test_role", rolesBeforeRevoke.get(0).name());

        // Verify the relation is in cache when cache is enabled
        if (enableCache && store instanceof RelationalEntityStore) {
          RelationalEntityStore relStore = (RelationalEntityStore) store;
          if (relStore.getCache() instanceof CaffeineEntityCache) {
            CaffeineEntityCache caffeineCache = (CaffeineEntityCache) relStore.getCache();
            List<Entity> cachedRoles =
                caffeineCache
                    .getCacheData()
                    .getIfPresent(
                        EntityCacheRelationKey.of(
                            schema.nameIdentifier(),
                            Entity.EntityType.SCHEMA,
                            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL));
            Assertions.assertNotNull(cachedRoles, "Cache should be populated after first fetch");
            Assertions.assertEquals(1, cachedRoles.size());
          }
        }

        // Simulate revokePrivilegesFromRole: update the role to remove the schema securable object.
        // RelationalEntityStore.update() calls cache.invalidate(roleIdent, ROLE) afterwards,
        // which must also invalidate the schema's METADATA_OBJECT_ROLE_REL cache entry.
        store.update(
            role.nameIdentifier(),
            RoleEntity.class,
            Entity.EntityType.ROLE,
            existing ->
                RoleEntity.builder()
                    .withId(existing.id())
                    .withName(existing.name())
                    .withNamespace(existing.namespace())
                    .withProperties(existing.properties())
                    .withAuditInfo(existing.auditInfo())
                    .withSecurableObjects(Lists.newArrayList()) // all privileges revoked
                    .build());

        // Verify METADATA_OBJECT_ROLE_REL cache is gone when cache is enabled
        if (enableCache && store instanceof RelationalEntityStore) {
          RelationalEntityStore relStore = (RelationalEntityStore) store;
          if (relStore.getCache() instanceof CaffeineEntityCache) {
            CaffeineEntityCache caffeineCache = (CaffeineEntityCache) relStore.getCache();
            List<Entity> cachedAfterRevoke =
                caffeineCache
                    .getCacheData()
                    .getIfPresent(
                        EntityCacheRelationKey.of(
                            schema.nameIdentifier(),
                            Entity.EntityType.SCHEMA,
                            SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL));
            Assertions.assertNull(
                cachedAfterRevoke,
                "METADATA_OBJECT_ROLE_REL cache must be invalidated after role update");
          }
        }

        // The key correctness check: listEntitiesByRelation must not return the revoked role
        List<RoleEntity> rolesAfterRevoke =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertTrue(
            rolesAfterRevoke.isEmpty(),
            "listEntitiesByRelation must return empty after revoking the last privilege from role");

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Reproduces the stale-read defect where granting a role access to an already-cached metadata
   * object is not reflected by {@code listEntitiesByRelation(METADATA_OBJECT_ROLE_REL)} until the
   * cache entry's TTL elapses.
   *
   * <p>Flow: create schema + roleA (bound to schema) + roleB (no securable object, never cached as
   * a binding role) -> list binding roles (warms cache; the reverse index maps roleA but not roleB)
   * -> update roleB to bind the schema (simulates grantPrivilegesToRole) -> list binding roles
   * again -> must immediately contain roleB.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testGrantPrivilegeInvalidatesMetadataObjectRoleRelCache(String type, boolean enableCache)
      throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "test_schema",
                auditInfo);
        store.put(schema, false);

        SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
        SecurableObject schemaObject =
            SecurableObjects.ofSchema(
                catalogObject, "test_schema", Lists.newArrayList(Privileges.UseSchema.allow()));

        // roleA is bound to the schema; roleB has no securable object, so it was never cached as a
        // binding role of any metadata object (its identifier is absent from the reverse index).
        RoleEntity roleA =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("roleA")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList(schemaObject))
                .build();
        store.put(roleA, false);

        RoleEntity roleB =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("roleB")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList())
                .build();
        store.put(roleB, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm the METADATA_OBJECT_ROLE_REL cache: schema -> [roleA]. The reverse index now holds
        // roleA -> schemaKey but NOT roleB, because roleB has never been served as a binding role.
        List<RoleEntity> rolesBeforeGrant =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertEquals(1, rolesBeforeGrant.size());
        Assertions.assertEquals("roleA", rolesBeforeGrant.get(0).name());

        // Simulate grantPrivilegesToRole: bind roleB to the schema. roleB was never cached, so the
        // role-side invalidation cannot reach the schema's relation cache entry through the reverse
        // index; RelationalEntityStore must explicitly invalidate it to avoid a stale read.
        store.update(
            roleB.nameIdentifier(),
            RoleEntity.class,
            Entity.EntityType.ROLE,
            existing ->
                RoleEntity.builder()
                    .withId(existing.id())
                    .withName(existing.name())
                    .withNamespace(existing.namespace())
                    .withProperties(existing.properties())
                    .withAuditInfo(existing.auditInfo())
                    .withSecurableObjects(Lists.newArrayList(schemaObject))
                    .build());

        // listBindingRoleNames(schema) must immediately reflect roleB after the grant.
        List<RoleEntity> rolesAfterGrant =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        List<String> roleNames =
            rolesAfterGrant.stream().map(RoleEntity::name).sorted().collect(Collectors.toList());
        Assertions.assertEquals(
            Lists.newArrayList("roleA", "roleB"),
            roleNames,
            "grant must be immediately visible via listBindingRoleNames");

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Guards the subtraction side of {@link
   * #testGrantPrivilegeInvalidatesMetadataObjectRoleRelCache}: after fully revoking a role's
   * privilege on an already-warmed metadata object, listBindingRoleNames must immediately drop the
   * role. The removed object is no longer in the updated entity, so this relies on the role-side
   * {@code invalidate(roleIdent, ROLE)} propagating to the object's relation cache via the reverse
   * index; the test pins that behavior so a future change to the reverse-index rule cannot silently
   * regress revoke.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testRevokeAllPrivilegesInvalidatesMetadataObjectRoleRelCache(
      String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "test_schema",
                auditInfo);
        store.put(schema, false);

        SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
        SecurableObject schemaObject =
            SecurableObjects.ofSchema(
                catalogObject, "test_schema", Lists.newArrayList(Privileges.UseSchema.allow()));

        RoleEntity role =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("test_role")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList(schemaObject))
                .build();
        store.put(role, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm the cache: schema -> [test_role].
        List<RoleEntity> rolesBeforeRevoke =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertEquals(1, rolesBeforeRevoke.size());
        Assertions.assertEquals("test_role", rolesBeforeRevoke.get(0).name());

        // Revoke all privileges on the schema (the object is removed from the role). The removed
        // object is absent from the updated entity, so it must be invalidated via the role side.
        store.update(
            role.nameIdentifier(),
            RoleEntity.class,
            Entity.EntityType.ROLE,
            existing ->
                RoleEntity.builder()
                    .withId(existing.id())
                    .withName(existing.name())
                    .withNamespace(existing.namespace())
                    .withProperties(existing.properties())
                    .withAuditInfo(existing.auditInfo())
                    .withSecurableObjects(Lists.newArrayList())
                    .build());

        List<RoleEntity> rolesAfterRevoke =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertTrue(
            rolesAfterRevoke.stream().noneMatch(r -> r.name().equals("test_role")),
            "revoke must be immediately visible: the role must no longer bind the schema");

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Guards the subtraction side: overriding a role so that a previously-bound, already-warmed
   * metadata object is dropped must make listBindingRoleNames immediately drop the role for that
   * object. The removed object is absent from the updated entity, so invalidation must come from
   * the role side via the reverse index.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testOverrideRemoveObjectInvalidatesMetadataObjectRoleRelCache(
      String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "test_schema",
                auditInfo);
        store.put(schema, false);

        SecurableObject catalogObject =
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
        SecurableObject schemaObject =
            SecurableObjects.ofSchema(
                catalogObject, "test_schema", Lists.newArrayList(Privileges.UseSchema.allow()));

        // role binds both catalog and schema.
        RoleEntity role =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("test_role")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList(catalogObject, schemaObject))
                .build();
        store.put(role, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm the cache: schema -> [test_role].
        List<RoleEntity> rolesBeforeOverride =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertEquals(1, rolesBeforeOverride.size());
        Assertions.assertEquals("test_role", rolesBeforeOverride.get(0).name());

        // Override to keep only the catalog object, dropping the schema object.
        store.update(
            role.nameIdentifier(),
            RoleEntity.class,
            Entity.EntityType.ROLE,
            existing ->
                RoleEntity.builder()
                    .withId(existing.id())
                    .withName(existing.name())
                    .withNamespace(existing.namespace())
                    .withProperties(existing.properties())
                    .withAuditInfo(existing.auditInfo())
                    .withSecurableObjects(Lists.newArrayList(catalogObject))
                    .build());

        List<RoleEntity> rolesAfterOverride =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertTrue(
            rolesAfterOverride.stream().noneMatch(r -> r.name().equals("test_role")),
            "override-remove must be immediately visible: the role must no longer bind the schema");

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Guards the subtraction side: deleting a role that bound an already-warmed metadata object must
   * make listBindingRoleNames immediately drop the role. deleteRole goes through {@code
   * store.delete} (not put/update), so the object-side invalidation must come from the role-side
   * {@code invalidate(roleIdent, ROLE)} via the reverse index.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testDeleteRoleInvalidatesMetadataObjectRoleRelCache(String type, boolean enableCache)
      throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "test_schema",
                auditInfo);
        store.put(schema, false);

        SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
        SecurableObject schemaObject =
            SecurableObjects.ofSchema(
                catalogObject, "test_schema", Lists.newArrayList(Privileges.UseSchema.allow()));

        RoleEntity role =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("test_role")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList(schemaObject))
                .build();
        store.put(role, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm the cache: schema -> [test_role].
        List<RoleEntity> rolesBeforeDelete =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertEquals(1, rolesBeforeDelete.size());
        Assertions.assertEquals("test_role", rolesBeforeDelete.get(0).name());

        // Delete the role; its binding must vanish from the warmed schema cache.
        store.delete(role.nameIdentifier(), Entity.EntityType.ROLE);

        List<RoleEntity> rolesAfterDelete =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertTrue(
            rolesAfterDelete.stream().noneMatch(r -> r.name().equals("test_role")),
            "deleteRole must be immediately visible: the role must no longer bind the schema");

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Covers the createRole path (which goes through {@code store.put}, not update): creating a role
   * that already carries securable objects must make listBindingRoleNames on those objects
   * immediately reflect the new role, even when the object's relation cache was warmed beforehand.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testCreateRoleWithSecurableObjectsInvalidatesMetadataObjectRoleRelCache(
      String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        CatalogEntity catalog =
            createCatalog(
                RandomIdGenerator.INSTANCE.nextId(),
                NamespaceUtil.ofCatalog("metalake"),
                "catalog",
                auditInfo);
        store.put(catalog, false);

        SchemaEntity schema =
            createSchemaEntity(
                RandomIdGenerator.INSTANCE.nextId(),
                Namespace.of("metalake", "catalog"),
                "test_schema",
                auditInfo);
        store.put(schema, false);

        SecurableObject catalogObject = SecurableObjects.ofCatalog("catalog", Lists.newArrayList());
        SecurableObject schemaObject =
            SecurableObjects.ofSchema(
                catalogObject, "test_schema", Lists.newArrayList(Privileges.UseSchema.allow()));

        RoleEntity roleA =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("roleA")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList(schemaObject))
                .build();
        store.put(roleA, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm the cache: schema -> [roleA].
        List<RoleEntity> rolesBeforeCreate =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        Assertions.assertEquals(1, rolesBeforeCreate.size());
        Assertions.assertEquals("roleA", rolesBeforeCreate.get(0).name());

        // Create roleB already bound to the schema (createRole goes through store.put).
        RoleEntity roleB =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("roleB")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList(schemaObject))
                .build();
        store.put(roleB, false);

        // listBindingRoleNames(schema) must immediately reflect roleB.
        List<RoleEntity> rolesAfterCreate =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.METADATA_OBJECT_ROLE_REL,
                schema.nameIdentifier(),
                Entity.EntityType.SCHEMA,
                true);
        List<String> roleNames =
            rolesAfterCreate.stream().map(RoleEntity::name).sorted().collect(Collectors.toList());
        Assertions.assertEquals(
            Lists.newArrayList("roleA", "roleB"),
            roleNames,
            "createRole with securable objects must be immediately visible");

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Covers the {@code grantRolesToUser} path: after a role is granted to a user that was never
   * cached against that role, listEntitiesByRelation(ROLE_USER_REL, roleIdent) must immediately
   * reflect the new user. Same defect shape as {@link
   * #testGrantPrivilegeInvalidatesMetadataObjectRoleRelCache}, on user.roleNames instead of
   * role.securableObjects.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testGrantRolesToUserInvalidatesRoleUserRelCache(String type, boolean enableCache)
      throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        RoleEntity roleA =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("roleA")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList())
                .build();
        store.put(roleA, false);

        UserEntity user1 =
            UserEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("user1")
                .withNamespace(AuthorizationUtils.ofUserNamespace("metalake"))
                .withRoleNames(Lists.newArrayList("roleA"))
                .withRoleIds(Lists.newArrayList(roleA.id()))
                .withAuditInfo(auditInfo)
                .build();
        store.put(user1, false);

        UserEntity user2 =
            UserEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("user2")
                .withNamespace(AuthorizationUtils.ofUserNamespace("metalake"))
                .withRoleNames(Lists.newArrayList())
                .withRoleIds(Lists.newArrayList())
                .withAuditInfo(auditInfo)
                .build();
        store.put(user2, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm the cache: roleA -> [user1].
        List<UserEntity> usersBeforeGrant =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.ROLE_USER_REL,
                roleA.nameIdentifier(),
                Entity.EntityType.ROLE,
                true);
        Assertions.assertEquals(1, usersBeforeGrant.size());
        Assertions.assertEquals("user1", usersBeforeGrant.get(0).name());

        // Simulate grantRolesToUser(roleA -> user2): store.update(user2, roleNames=[roleA]).
        store.update(
            user2.nameIdentifier(),
            UserEntity.class,
            Entity.EntityType.USER,
            existing ->
                UserEntity.builder()
                    .withId(existing.id())
                    .withName(existing.name())
                    .withNamespace(existing.namespace())
                    .withRoleNames(Lists.newArrayList("roleA"))
                    .withRoleIds(Lists.newArrayList(roleA.id()))
                    .withAuditInfo(auditInfo)
                    .build());

        // roleA -> users must immediately reflect user2.
        List<UserEntity> usersAfterGrant =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.ROLE_USER_REL,
                roleA.nameIdentifier(),
                Entity.EntityType.ROLE,
                true);
        List<String> names =
            usersAfterGrant.stream().map(UserEntity::name).sorted().collect(Collectors.toList());
        Assertions.assertEquals(
            Lists.newArrayList("user1", "user2"),
            names,
            "grantRolesToUser must be immediately visible via role->users");

      } finally {
        destroy(type);
      }
    }
  }

  /**
   * Covers the {@code grantRolesToGroup} path: after a role is granted to a group that was never
   * cached against that role, listEntitiesByRelation(ROLE_GROUP_REL, roleIdent) must immediately
   * reflect the new group.
   */
  @ParameterizedTest
  @MethodSource("storageProvider")
  void testGrantRolesToGroupInvalidatesRoleGroupRelCache(String type, boolean enableCache)
      throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      try {
        store.initialize(config);

        BaseMetalake metalake =
            createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
        store.put(metalake, false);

        RoleEntity roleA =
            RoleEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("roleA")
                .withNamespace(AuthorizationUtils.ofRoleNamespace("metalake"))
                .withProperties(null)
                .withAuditInfo(auditInfo)
                .withSecurableObjects(Lists.newArrayList())
                .build();
        store.put(roleA, false);

        GroupEntity group1 =
            GroupEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("group1")
                .withNamespace(AuthorizationUtils.ofGroupNamespace("metalake"))
                .withRoleNames(Lists.newArrayList("roleA"))
                .withRoleIds(Lists.newArrayList(roleA.id()))
                .withAuditInfo(auditInfo)
                .build();
        store.put(group1, false);

        GroupEntity group2 =
            GroupEntity.builder()
                .withId(RandomIdGenerator.INSTANCE.nextId())
                .withName("group2")
                .withNamespace(AuthorizationUtils.ofGroupNamespace("metalake"))
                .withRoleNames(Lists.newArrayList())
                .withRoleIds(Lists.newArrayList())
                .withAuditInfo(auditInfo)
                .build();
        store.put(group2, false);

        SupportsRelationOperations relationOperations = (SupportsRelationOperations) store;

        // Warm the cache: roleA -> [group1].
        List<GroupEntity> groupsBeforeGrant =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                roleA.nameIdentifier(),
                Entity.EntityType.ROLE,
                true);
        Assertions.assertEquals(1, groupsBeforeGrant.size());
        Assertions.assertEquals("group1", groupsBeforeGrant.get(0).name());

        // Simulate grantRolesToGroup(roleA -> group2): store.update(group2, roleNames=[roleA]).
        store.update(
            group2.nameIdentifier(),
            GroupEntity.class,
            Entity.EntityType.GROUP,
            existing ->
                GroupEntity.builder()
                    .withId(existing.id())
                    .withName(existing.name())
                    .withNamespace(existing.namespace())
                    .withRoleNames(Lists.newArrayList("roleA"))
                    .withRoleIds(Lists.newArrayList(roleA.id()))
                    .withAuditInfo(auditInfo)
                    .build());

        // roleA -> groups must immediately reflect group2.
        List<GroupEntity> groupsAfterGrant =
            relationOperations.listEntitiesByRelation(
                SupportsRelationOperations.Type.ROLE_GROUP_REL,
                roleA.nameIdentifier(),
                Entity.EntityType.ROLE,
                true);
        List<String> names =
            groupsAfterGrant.stream().map(GroupEntity::name).sorted().collect(Collectors.toList());
        Assertions.assertEquals(
            Lists.newArrayList("group1", "group2"),
            names,
            "grantRolesToGroup must be immediately visible via role->groups");

      } finally {
        destroy(type);
      }
    }
  }

  private FunctionEntity createFunctionEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    FunctionParam param1 = FunctionParams.of("param1", Types.IntegerType.get());
    FunctionParam param2 = FunctionParams.of("param2", Types.StringType.get());
    FunctionImpl functionImpl =
        FunctionImpls.ofSql(FunctionImpl.RuntimeType.SPARK, "SELECT param1 + 1");
    FunctionDefinition definition =
        FunctionDefinitions.of(
            new FunctionParam[] {param1, param2},
            Types.IntegerType.get(),
            new FunctionImpl[] {functionImpl});

    return FunctionEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("test function comment")
        .withFunctionType(FunctionType.SCALAR)
        .withDeterministic(false)
        .withDefinitions(new FunctionDefinition[] {definition})
        .withAuditInfo(auditInfo)
        .build();
  }
}
