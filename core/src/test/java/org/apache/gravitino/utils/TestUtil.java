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

package org.apache.gravitino.utils;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.ModelEntity;
import org.apache.gravitino.meta.ModelVersionEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.storage.RandomIdGenerator;

/**
 * Utility class providing factory methods for creating mock and test instances of various metadata
 * entities used in unit tests.
 *
 * <p>This class includes shortcut methods for constructing entities such as {@link BaseMetalake},
 * {@link CatalogEntity}, {@link SchemaEntity}, {@link TableEntity}, {@link ModelEntity}, {@link
 * FilesetEntity}, {@link TopicEntity}, {@link TagEntity}, {@link UserEntity}, {@link GroupEntity},
 * {@link RoleEntity}, and {@link ModelVersionEntity}. It also includes methods to create mock
 * {@link ColumnEntity} and {@link SecurableObject} instances for use in mocking behavior-dependent
 * components.
 *
 * <p>These utilities are intended for testing purposes only and rely on in-memory mock objects or
 * randomly generated IDs to simulate real entities.
 *
 * <p>Note: This class is not thread-safe.
 */
public class TestUtil {
  // Random ID generator used to generate IDs for test entities.
  private static RandomIdGenerator generator = new RandomIdGenerator();

  /**
   * Shorthand for creating a test metalake entity with default values.
   *
   * @return The test {@link BaseMetalake} entity.
   */
  public static BaseMetalake getTestMetalake() {
    return getTestMetalake(generator.nextId(), "test_metalake", "metalake entity test");
  }

  /**
   * Get the test metalake entity with the given ID, name, and comment.
   *
   * @param id The ID of the metalake entity.
   * @param name The name of the metalake entity.
   * @param comment The comment of the metalake entity.
   * @return The test {@link BaseMetalake} entity.
   */
  public static BaseMetalake getTestMetalake(long id, String name, String comment) {
    return BaseMetalake.builder()
        .withId(id)
        .withName(name)
        .withVersion(SchemaVersion.V_0_1)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  /**
   * Shorthand for creating a test catalog entity with default values.
   *
   * @return The test {@link CatalogEntity} entity.
   */
  public static CatalogEntity getTestCatalogEntity() {
    return getTestCatalogEntity(
        generator.nextId(), "test_catalog", Namespace.of("m1"), "hive", "catalog entity test");
  }

  /**
   * Returns a test catalog entity with the given ID, name, namespace, provider, and comment.
   *
   * @param id The ID of the catalog entity.
   * @param name The name of the catalog entity.
   * @param namespace The namespace of the catalog entity.
   * @param provider The provider of the catalog entity.
   * @param comment The comment of the catalog entity.
   * @return The test {@link CatalogEntity} entity.
   */
  public static CatalogEntity getTestCatalogEntity(
      long id, String name, Namespace namespace, String provider, String comment) {
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider(provider)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  /**
   * Shorthand for creating a test schema entity with default values.
   *
   * @return The test {@link SchemaEntity} entity.
   */
  public static SchemaEntity getTestSchemaEntity() {
    return getTestSchemaEntity(
        generator.nextId(), "test_schema", Namespace.of("m1", "c1"), "schema entity test");
  }

  /**
   * Returns a test schema entity with the given ID, name, namespace, and comment.
   *
   * @param id The ID of the schema entity.
   * @param name The name of the schema entity.
   * @param namespace The namespace of the schema entity.
   * @param comment The comment of the schema entity.
   * @return The test {@link SchemaEntity} entity.
   */
  public static SchemaEntity getTestSchemaEntity(
      long id, String name, Namespace namespace, String comment) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  /**
   * Shorthand for creating a test model entity with default values.
   *
   * @return The test {@link TableEntity} entity.
   */
  public static ModelEntity getTestModelEntity() {
    return getTestModelEntity(generator.nextId(), "test_model", Namespace.of("m1", "c1", "s1"));
  }

  /**
   * Returns a test model entity with the given ID, name, and namespace.
   *
   * @param id The ID of the model entity.
   * @param name The name of the model entity.
   * @param namespace The namespace of the model entity.
   * @return The test {@link ModelEntity} entity.
   */
  public static ModelEntity getTestModelEntity(long id, String name, Namespace namespace) {
    return ModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withLatestVersion(1)
        .withAuditInfo(getTestAuditInfo())
        .build();
  }

  /**
   * Shorthand for creating a test table entity with default values.
   *
   * @return The test {@link TableEntity} entity.
   */
  public static TableEntity getTestTableEntity() {
    return getTestTableEntity(generator.nextId(), "test_table", Namespace.of("m1", "c2", "s2"));
  }

  /**
   * Returns a test table entity with the given ID, name, and namespace.
   *
   * @param id The ID of the table entity.
   * @param name The name of the table entity.
   * @param namespace The namespace of the table entity.
   * @return The test {@link TableEntity} entity.
   */
  public static TableEntity getTestTableEntity(long id, String name, Namespace namespace) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(getTestAuditInfo())
        .withNamespace(namespace)
        .withColumns(ImmutableList.of(getMockColumnEntity()))
        .build();
  }

  /**
   * Shorthand for creating a test fileset entity with default values.
   *
   * @return The test {@link FilesetEntity} entity.
   */
  public static FilesetEntity getTestFileSetEntity() {
    return getTestFileSetEntity(
        generator.nextId(),
        "fileset_test",
        "file:///tmp/fileset_test",
        Namespace.of("m1", "c3", "s3"),
        "fileset entity test",
        Fileset.Type.EXTERNAL);
  }

  /**
   * Returns a test fileset entity with the given ID, name, storage location, namespace, and
   * comment.
   *
   * @param id The ID of the fileset entity.
   * @param name The name of the fileset entity.
   * @param storageLocation The storage location of the fileset entity.
   * @param namespace The namespace of the fileset entity.
   * @param comment The comment of the fileset entity.
   * @param type The type of the fileset entity.
   * @return The test {@link FilesetEntity} entity.
   */
  public static FilesetEntity getTestFileSetEntity(
      long id,
      String name,
      String storageLocation,
      Namespace namespace,
      String comment,
      Fileset.Type type) {
    return FilesetEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withStorageLocations(ImmutableMap.of("location1", storageLocation))
        .withFilesetType(type)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  /**
   * Returns a test topic entity with default values.
   *
   * @return The test {@link TopicEntity} entity.
   */
  public static TopicEntity getTestTopicEntity() {
    return getTestTopicEntity(
        generator.nextId(), "topic_test", Namespace.of("m1", "c4", "s4"), "topic entity test");
  }

  /**
   * Returns a test topic entity with the given ID, name, namespace, and comment.
   *
   * @param id The ID of the topic entity.
   * @param name The name of the topic entity.
   * @param namespace The namespace of the topic entity.
   * @param comment The comment of the topic entity.
   * @return The test {@link TopicEntity} entity.
   */
  public static TopicEntity getTestTopicEntity(
      long id, String name, Namespace namespace, String comment) {
    return TopicEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  /**
   * Shorthand for creating a test tag entity with default values.
   *
   * @return The test {@link TagEntity} entity.
   */
  public static TagEntity getTestTagEntity() {
    return getTestTagEntity(generator.nextId(), "tag_test", Namespace.of("m1"), "tag entity test");
  }

  /**
   * Returns a test tag entity with the given ID, name, namespace, and comment.
   *
   * @param id The ID of the tag entity.
   * @param name The name of the tag entity.
   * @param namespace The namespace of the tag entity.
   * @param comment The comment of the tag entity.
   * @return The test {@link TagEntity} entity.
   */
  public static TagEntity getTestTagEntity(
      long id, String name, Namespace namespace, String comment) {
    return TagEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(getTestAuditInfo())
        .withComment(comment)
        .withProperties(ImmutableMap.of())
        .build();
  }

  /**
   * Returns a column entity with default values.
   *
   * @return The test {@link ColumnEntity} entity.
   */
  public static ColumnEntity getMockColumnEntity() {
    ColumnEntity mockColumn = mock(ColumnEntity.class);
    when(mockColumn.name()).thenReturn("filed1");
    when(mockColumn.dataType()).thenReturn(Types.StringType.get());
    when(mockColumn.nullable()).thenReturn(false);
    when(mockColumn.auditInfo()).thenReturn(getTestAuditInfo());

    return mockColumn;
  }

  /**
   * Returns an audit info entity with default values.
   *
   * @return The test {@link AuditInfo} entity.
   */
  public static AuditInfo getTestAuditInfo() {
    return AuditInfo.builder()
        .withCreator("admin")
        .withCreateTime(Instant.now())
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.now())
        .build();
  }

  /**
   * Shorthand for creating a test user entity with default values.
   *
   * @return The test {@link UserEntity} entity.
   */
  public static UserEntity getTestUserEntity() {
    return getTestUserEntity(
        generator.nextId(), "test_user", "test_metalake", ImmutableList.of(1L));
  }

  /**
   * Returns a test user entity with the given ID, name, and metalake.
   *
   * @param id The ID of the user entity.
   * @param name The name of the user entity.
   * @param metalake The metalake of the user entity.
   * @param roles The roles of the user entity.
   * @return The test {@link UserEntity} entity.
   */
  public static UserEntity getTestUserEntity(
      long id, String name, String metalake, List<Long> roles) {
    return UserEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofUser(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withRoleIds(roles)
        .build();
  }

  /**
   * Shorthand for creating a test group entity with default values.
   *
   * @return The test {@link GroupEntity} entity.
   */
  public static GroupEntity getTestGroupEntity() {
    return getTestGroupEntity(
        generator.nextId(), "test_group", "test_metalake", ImmutableList.of("role1", "role2"));
  }

  /**
   * Returns a test group entity with the given ID, name, metalake, and roles.
   *
   * @param id The ID of the group entity.
   * @param name The name of the group entity.
   * @param metalake The metalake of the group entity.
   * @param roles The roles of the group entity.
   * @return The test {@link GroupEntity} entity.
   */
  public static GroupEntity getTestGroupEntity(
      long id, String name, String metalake, List<String> roles) {
    return GroupEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofGroup(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withRoleNames(roles)
        .build();
  }

  /**
   * Shorthand for creating a test role entity with default values.
   *
   * @return The test {@link RoleEntity} entity.
   */
  public static RoleEntity getTestRoleEntity() {
    return getTestRoleEntity(generator.nextId(), "test_role", "test_metalake");
  }

  /**
   * Returns a test role entity with the given ID, name, and metalake.
   *
   * @param id The ID of the role entity.
   * @param name The name of the role entity.
   * @param metalake The metalake of the role entity.
   * @return The test {@link RoleEntity} entity.
   */
  public static RoleEntity getTestRoleEntity(long id, String name, String metalake) {
    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofRole(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withSecurableObjects(ImmutableList.of(getMockSecurableObject()))
        .build();
  }

  /**
   * Returns a mock securable object with default values.
   *
   * @return The test {@link SecurableObject} entity.
   */
  public static SecurableObject getMockSecurableObject() {
    SecurableObject mockObject = mock(SecurableObject.class);
    when(mockObject.privileges())
        .thenReturn(
            ImmutableList.of(Privileges.CreateSchema.allow(), Privileges.CreateTable.allow()));

    return mockObject;
  }

  /**
   * Shorthand for creating a test model version entity with default values.
   *
   * @return The test {@link ModelVersionEntity} entity.
   */
  public static ModelVersionEntity getTestModelVersionEntity() {
    return getTestModelVersionEntity(
        NameIdentifier.of("m1", "c1", "s1", "model-version-test"),
        1,
        "uri",
        ImmutableMap.of("key1", "value1"),
        "model version entity test",
        ImmutableList.of("alias1", "alias2"));
  }

  /**
   * Returns a test model version entity with the given model identifier, version, uri, properties,
   * comment, and aliases.
   *
   * @param modelIdentifier The model identifier of the model version entity.
   * @param version The version of the model version entity.
   * @param uri The uri of the model version entity.
   * @param properties The properties of the model version entity.
   * @param comment The comment of the model version entity.
   * @param aliases The aliases of the model version entity.
   * @return The test {@link ModelVersionEntity} entity.
   */
  public static ModelVersionEntity getTestModelVersionEntity(
      NameIdentifier modelIdentifier,
      int version,
      String uri,
      Map<String, String> properties,
      String comment,
      List<String> aliases) {
    return ModelVersionEntity.builder()
        .withVersion(version)
        .withModelIdentifier(modelIdentifier)
        .withUri(uri)
        .withProperties(properties)
        .withComment(comment)
        .withAuditInfo(getTestAuditInfo())
        .withAliases(aliases)
        .build();
  }
}
