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
import java.util.ArrayList;
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

public class TestUtil {
  private static RandomIdGenerator generator = new RandomIdGenerator();

  public static <T> List<T> toList(Iterable<T> iterable) {
    List<T> list = new ArrayList<>();
    for (T item : iterable) {
      list.add(item);
    }
    return list;
  }

  public static BaseMetalake getTestMetalake() {
    return getTestMetalake(generator.nextId(), "test_metalake", "metalake entity test");
  }

  /**
   * Get
   *
   * @param id
   * @param name
   * @param comment
   * @return
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

  public static CatalogEntity getTestCatalogEntity() {
    return getTestCatalogEntity(
        generator.nextId(), "test_catalog", Namespace.of("m1"), "hive", "catalog entity test");
  }

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

  public static SchemaEntity getTestSchemaEntity() {
    return getTestSchemaEntity(
        generator.nextId(), "test_schema", Namespace.of("m1", "c1"), "schema entity test");
  }

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

  public static ModelEntity getTestModelEntity() {
    return getTestModelEntity(generator.nextId(), "test_model", Namespace.of("m1", "c1", "s1"));
  }

  public static ModelEntity getTestModelEntity(long id, String name, Namespace namespace) {
    return ModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withLatestVersion(1)
        .withAuditInfo(getTestAuditInfo())
        .build();
  }

  public static TableEntity getTestTableEntity() {
    return getTestTableEntity(generator.nextId(), "test_table", Namespace.of("m1", "c2", "s2"));
  }

  public static TableEntity getTestTableEntity(long id, String name, Namespace namespace) {
    return TableEntity.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(getTestAuditInfo())
        .withNamespace(namespace)
        .withColumns(ImmutableList.of(getMockColumnEntity()))
        .build();
  }

  public static FilesetEntity getTestFileSetEntity() {
    return getTestFileSetEntity(
        generator.nextId(),
        "fileset_test",
        "file:///tmp/fileset_test",
        Namespace.of("m1", "c3", "s3"),
        "fileset entity test",
        Fileset.Type.EXTERNAL);
  }

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

  public static TopicEntity getTestTopicEntity() {
    return getTestTopicEntity(
        generator.nextId(), "topic_test", Namespace.of("m1", "c4", "s4"), "topic entity test");
  }

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

  public static TagEntity getTestTagEntity() {
    return getTestTagEntity(generator.nextId(), "tag_test", Namespace.of("m1"), "tag entity test");
  }

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

  public static ColumnEntity getMockColumnEntity() {
    ColumnEntity mockColumn = mock(ColumnEntity.class);
    when(mockColumn.name()).thenReturn("filed1");
    when(mockColumn.dataType()).thenReturn(Types.StringType.get());
    when(mockColumn.nullable()).thenReturn(false);
    when(mockColumn.auditInfo()).thenReturn(getTestAuditInfo());

    return mockColumn;
  }

  public static AuditInfo getTestAuditInfo() {
    return AuditInfo.builder()
        .withCreator("admin")
        .withCreateTime(Instant.now())
        .withLastModifier("admin")
        .withLastModifiedTime(Instant.now())
        .build();
  }

  public static UserEntity getTestUserEntity() {
    return getTestUserEntity(generator.nextId(), "test_user", "test_metalake");
  }

  public static UserEntity getTestUserEntity(long id, String name, String metalake) {
    return UserEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofUser(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withRoleIds(ImmutableList.of(1L, 2L, 3L))
        .build();
  }

  public static GroupEntity getTestGroupEntity() {
    return getTestGroupEntity(
        generator.nextId(), "test_group", "test_metalake", ImmutableList.of("role1", "role2"));
  }

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

  public static RoleEntity getTestRoleEntity() {
    return getTestRoleEntity(generator.nextId(), "test_role", "test_metalake");
  }

  public static RoleEntity getTestRoleEntity(long id, String name, String metalake) {
    return RoleEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(NamespaceUtil.ofRole(metalake))
        .withAuditInfo(getTestAuditInfo())
        .withSecurableObjects(ImmutableList.of(getMockSecurableObject()))
        .build();
  }

  public static SecurableObject getMockSecurableObject() {
    SecurableObject mockObject = mock(SecurableObject.class);
    when(mockObject.privileges())
        .thenReturn(
            ImmutableList.of(Privileges.CreateSchema.allow(), Privileges.CreateTable.allow()));

    return mockObject;
  }

  public static ModelVersionEntity getTestModelVersionEntity() {
    return getTestModelVersionEntity(
        NameIdentifier.of("m1", "c1", "s1", "model-version-test"),
        1,
        "uri",
        ImmutableMap.of("key1", "value1"),
        "model version entity test",
        ImmutableList.of("alias1", "alias2"));
  }

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
