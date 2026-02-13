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
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GenericEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
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
  void testViewCacheInvalidation(String type, boolean enableCache) throws Exception {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(enableCache);
    init(type, config);

    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();

    try (EntityStore store = EntityStoreFactory.createEntityStore(config)) {
      store.initialize(config);
      if (!enableCache) {
        return;
      }

      BaseMetalake metalake =
          createBaseMakeLake(RandomIdGenerator.INSTANCE.nextId(), "metalake", auditInfo);
      store.put(metalake, false);

      CatalogEntity catalog =
          createCatalog(
              RandomIdGenerator.INSTANCE.nextId(), Namespace.of("metalake"), "catalog", auditInfo);
      store.put(catalog, false);

      SchemaEntity schema =
          createSchemaEntity(
              RandomIdGenerator.INSTANCE.nextId(),
              Namespace.of("metalake", "catalog"),
              "schema",
              auditInfo);
      store.put(schema, false);

      // Create a view as GenericEntity with namespace [metalake, catalog, schema]
      Namespace viewNamespace = Namespace.of("metalake", "catalog", "schema");
      GenericEntity view =
          GenericEntity.builder()
              .withId(RandomIdGenerator.INSTANCE.nextId())
              .withName("test_view")
              .withNamespace(viewNamespace)
              .withEntityType(Entity.EntityType.VIEW)
              .build();
      store.put(view, false);

      // Load view into cache
      NameIdentifier viewIdent = NameIdentifier.of(viewNamespace, "test_view");
      GenericEntity loadedView = store.get(viewIdent, Entity.EntityType.VIEW, GenericEntity.class);
      Assertions.assertNotNull(loadedView);

      // Verify reverse index has correct NameIdentifier with full namespace
      ReverseIndexCache reverseIndexCache =
          ((CaffeineEntityCache) ((RelationalEntityStore) store).getCache()).getReverseIndex();
      List<EntityCacheKey> reverseIndexValue =
          reverseIndexCache.get(viewIdent, Entity.EntityType.VIEW);
      Assertions.assertNotNull(
          reverseIndexValue,
          "Reverse index should contain entry for view with full namespace [metalake, catalog, schema, test_view]");
      Assertions.assertTrue(
          reverseIndexValue.stream()
              .anyMatch(key -> key.identifier().equals(schema.nameIdentifier())),
          "Reverse index should include parent schema");

      // Update schema - should invalidate view cache
      SchemaEntity updatedSchema =
          SchemaEntity.builder()
              .withId(schema.id())
              .withNamespace(schema.namespace())
              .withName("schema_renamed")
              .withAuditInfo(auditInfo)
              .build();
      store.update(
          schema.nameIdentifier(),
          SchemaEntity.class,
          Entity.EntityType.SCHEMA,
          e -> updatedSchema);

      // Verify reverse index entry was removed due to cache invalidation
      reverseIndexValue = reverseIndexCache.get(viewIdent, Entity.EntityType.VIEW);
      Assertions.assertNull(
          reverseIndexValue,
          "Reverse index should be cleared after parent schema update (cache invalidation)");

      // Reload view to repopulate cache and reverse index
      loadedView = store.get(viewIdent, Entity.EntityType.VIEW, GenericEntity.class);
      Assertions.assertNotNull(loadedView);

      // Verify reverse index is repopulated with updated schema
      reverseIndexValue = reverseIndexCache.get(viewIdent, Entity.EntityType.VIEW);
      Assertions.assertNotNull(reverseIndexValue);
      Assertions.assertTrue(
          reverseIndexValue.stream()
              .anyMatch(key -> key.identifier().equals(updatedSchema.nameIdentifier())),
          "Reverse index should now include updated schema");

      // Delete view and verify reverse index cleanup
      store.delete(viewIdent, Entity.EntityType.VIEW);
      Assertions.assertFalse(store.exists(viewIdent, Entity.EntityType.VIEW));
      reverseIndexValue = reverseIndexCache.get(viewIdent, Entity.EntityType.VIEW);
      Assertions.assertNull(reverseIndexValue, "Reverse index should be cleaned up after deletion");

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
}
