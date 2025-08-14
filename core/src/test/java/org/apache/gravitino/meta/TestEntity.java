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
package org.apache.gravitino.meta;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Field;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.file.Fileset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestEntity {
  private final Instant now = Instant.now();

  private final SchemaVersion version = SchemaVersion.V_0_1;
  private final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("test").withCreateTime(now).build();

  // Metalake test data
  private final Long metalakeId = 1L;
  private final String metalakeName = "testMetalake";
  private final Map<String, String> map = ImmutableMap.of("k1", "v1", "k2", "v2");

  // Catalog test data
  private final Long catalogId = 1L;
  private final String catalogName = "testCatalog";
  private final Catalog.Type type = Catalog.Type.RELATIONAL;
  private final String provider = "test";

  // Schema test data
  private final Long schemaId = 1L;
  private final String schemaName = "testSchema";

  // Table test data
  private final Long tableId = 1L;
  private final String tableName = "testTable";

  // File test data
  private final Long fileId = 1L;
  private final String fileName = "testFile";

  // Topic test data
  private final Long topicId = 1L;
  private final String topicName = "testTopic";

  // User test data
  private final Long userId = 1L;
  private final String userName = "testUser";

  // Group test data
  private final Long groupId = 1L;
  private final String groupName = "testGroup";

  // Role test data
  private final Long roleId = 1L;
  private final String roleName = "testRole";

  @Test
  public void testMetalake() {
    BaseMetalake metalake =
        BaseMetalake.builder()
            .withId(metalakeId)
            .withName(metalakeName)
            .withAuditInfo(auditInfo)
            .withProperties(map)
            .withVersion(version)
            .build();

    Map<Field, Object> fields = metalake.fields();
    Assertions.assertEquals(metalakeId, fields.get(BaseMetalake.ID));
    Assertions.assertEquals(metalakeName, fields.get(BaseMetalake.NAME));
    Assertions.assertEquals(map, fields.get(BaseMetalake.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(BaseMetalake.AUDIT_INFO));
    Assertions.assertNull(fields.get(BaseMetalake.COMMENT));
    Assertions.assertEquals(version, fields.get(BaseMetalake.SCHEMA_VERSION));
  }

  @Test
  public void testCatalog() {
    String catalogComment = "testComment";
    CatalogEntity testCatalog =
        CatalogEntity.builder()
            .withId(catalogId)
            .withName(catalogName)
            .withComment(catalogComment)
            .withType(type)
            .withProvider(provider)
            .withProperties(map)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testCatalog.fields();
    Assertions.assertEquals(catalogId, fields.get(CatalogEntity.ID));
    Assertions.assertEquals(catalogName, fields.get(CatalogEntity.NAME));
    Assertions.assertEquals(catalogComment, fields.get(CatalogEntity.COMMENT));
    Assertions.assertEquals(type, fields.get(CatalogEntity.TYPE));
    Assertions.assertEquals(map, fields.get(CatalogEntity.PROPERTIES));
    Assertions.assertEquals(auditInfo, fields.get(CatalogEntity.AUDIT_INFO));
  }

  @Test
  public void testSchema() {
    SchemaEntity testSchema =
        SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .build();

    Map<Field, Object> fields = testSchema.fields();
    Assertions.assertEquals(schemaId, fields.get(SchemaEntity.ID));
    Assertions.assertEquals(schemaName, fields.get(SchemaEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(SchemaEntity.AUDIT_INFO));

    SchemaEntity testSchema1 =
        SchemaEntity.builder()
            .withId(schemaId)
            .withName(schemaName)
            .withAuditInfo(auditInfo)
            .withComment("testComment")
            .withProperties(map)
            .build();
    Map<Field, Object> fields1 = testSchema1.fields();
    Assertions.assertEquals("testComment", fields1.get(SchemaEntity.COMMENT));
    Assertions.assertEquals(map, fields1.get(SchemaEntity.PROPERTIES));
  }

  @Test
  public void testTable() {
    TableEntity testTable =
        TableEntity.builder().withId(tableId).withName(tableName).withAuditInfo(auditInfo).build();

    Map<Field, Object> fields = testTable.fields();
    Assertions.assertEquals(tableId, fields.get(TableEntity.ID));
    Assertions.assertEquals(tableName, fields.get(TableEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(TableEntity.AUDIT_INFO));
  }

  @Test
  public void testFile() {
    FilesetEntity testFile =
        FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withAuditInfo(auditInfo)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "testLocation"))
            .withProperties(map)
            .build();

    Map<Field, Object> fields = testFile.fields();
    Assertions.assertEquals(fileId, fields.get(FilesetEntity.ID));
    Assertions.assertEquals(fileName, fields.get(FilesetEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(FilesetEntity.AUDIT_INFO));
    Assertions.assertEquals(Fileset.Type.MANAGED, fields.get(FilesetEntity.TYPE));
    Assertions.assertEquals(map, fields.get(FilesetEntity.PROPERTIES));
    Assertions.assertNull(fields.get(FilesetEntity.COMMENT));
    Assertions.assertEquals(
        "testLocation",
        ((Map<String, String>) fields.get(FilesetEntity.STORAGE_LOCATIONS))
            .get(LOCATION_NAME_UNKNOWN));

    FilesetEntity testFile1 =
        FilesetEntity.builder()
            .withId(fileId)
            .withName(fileName)
            .withAuditInfo(auditInfo)
            .withFilesetType(Fileset.Type.MANAGED)
            .withComment("testComment")
            .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "testLocation"))
            .build();
    Assertions.assertEquals("testComment", testFile1.comment());
    Assertions.assertNull(testFile1.properties());

    Throwable exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                FilesetEntity.builder()
                    .withId(fileId)
                    .withName(fileName)
                    .withAuditInfo(auditInfo)
                    .withFilesetType(Fileset.Type.EXTERNAL)
                    .withProperties(map)
                    .withComment("testComment")
                    .build());
    Assertions.assertEquals(
        "The storage locations of the fileset entity must not be empty.", exception.getMessage());
  }

  @Test
  public void testTopic() {
    TopicEntity testTopic =
        TopicEntity.builder()
            .withId(topicId)
            .withName(topicName)
            .withAuditInfo(auditInfo)
            .withComment("test topic comment")
            .withProperties(map)
            .build();

    Map<Field, Object> fields = testTopic.fields();
    Assertions.assertEquals(topicId, fields.get(TopicEntity.ID));
    Assertions.assertEquals(topicName, fields.get(TopicEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(TopicEntity.AUDIT_INFO));
    Assertions.assertEquals("test topic comment", fields.get(TopicEntity.COMMENT));
    Assertions.assertEquals(map, fields.get(TopicEntity.PROPERTIES));

    TopicEntity testTopic1 =
        TopicEntity.builder().withId(topicId).withName(topicName).withAuditInfo(auditInfo).build();
    Assertions.assertNull(testTopic1.comment());
    Assertions.assertNull(testTopic1.properties());
  }

  @Test
  public void testUser() {
    UserEntity testUserEntity =
        UserEntity.builder()
            .withId(userId)
            .withName(userName)
            .withAuditInfo(auditInfo)
            .withRoleNames(Lists.newArrayList("role"))
            .build();

    Map<Field, Object> fields = testUserEntity.fields();
    Assertions.assertEquals(userId, fields.get(UserEntity.ID));
    Assertions.assertEquals(userName, fields.get(UserEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(UserEntity.AUDIT_INFO));
    Assertions.assertEquals(Lists.newArrayList("role"), fields.get(UserEntity.ROLE_NAMES));

    UserEntity testUserEntityWithoutFields =
        UserEntity.builder().withId(userId).withName(userName).withAuditInfo(auditInfo).build();

    Assertions.assertNull(testUserEntityWithoutFields.roles());
  }

  @Test
  public void testGroup() {
    GroupEntity group =
        GroupEntity.builder()
            .withId(groupId)
            .withName(groupName)
            .withAuditInfo(auditInfo)
            .withRoleNames(Lists.newArrayList("role"))
            .build();
    Map<Field, Object> fields = group.fields();
    Assertions.assertEquals(groupId, fields.get(GroupEntity.ID));
    Assertions.assertEquals(groupName, fields.get(GroupEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(GroupEntity.AUDIT_INFO));
    Assertions.assertEquals(Lists.newArrayList("role"), fields.get(GroupEntity.ROLE_NAMES));

    GroupEntity groupWithoutFields =
        GroupEntity.builder().withId(userId).withName(userName).withAuditInfo(auditInfo).build();

    Assertions.assertNull(groupWithoutFields.roles());
  }

  @Test
  public void testRole() {
    RoleEntity role =
        RoleEntity.builder()
            .withId(1L)
            .withName(roleName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(
                Lists.newArrayList(
                    SecurableObjects.ofCatalog(
                        catalogName, Lists.newArrayList(Privileges.UseCatalog.allow()))))
            .withProperties(map)
            .build();

    Map<Field, Object> fields = role.fields();
    Assertions.assertEquals(roleId, fields.get(RoleEntity.ID));
    Assertions.assertEquals(roleName, fields.get(RoleEntity.NAME));
    Assertions.assertEquals(auditInfo, fields.get(RoleEntity.AUDIT_INFO));
    Assertions.assertEquals(map, fields.get(RoleEntity.PROPERTIES));
    Assertions.assertEquals(
        Lists.newArrayList(
            SecurableObjects.ofCatalog(
                catalogName, Lists.newArrayList(Privileges.UseCatalog.allow()))),
        fields.get(RoleEntity.SECURABLE_OBJECTS));

    RoleEntity roleWithoutFields =
        RoleEntity.builder()
            .withId(1L)
            .withName(roleName)
            .withAuditInfo(auditInfo)
            .withSecurableObjects(
                Lists.newArrayList(
                    SecurableObjects.ofCatalog(
                        catalogName, Lists.newArrayList(Privileges.UseCatalog.allow()))))
            .build();
    Assertions.assertNull(roleWithoutFields.properties());
  }

  @Test
  public void testTag() {
    Map<String, String> prop = ImmutableMap.of("k1", "v1", "k2", "v2");

    TagEntity tag1 =
        TagEntity.builder()
            .withId(1L)
            .withName("tag1")
            .withComment("comment")
            .withProperties(prop)
            .withAuditInfo(auditInfo)
            .build();
    Assertions.assertEquals(1L, tag1.id());
    Assertions.assertEquals("tag1", tag1.name());
    Assertions.assertEquals("comment", tag1.comment());
    Assertions.assertEquals(prop, tag1.properties());
    Assertions.assertEquals(auditInfo, tag1.auditInfo());

    TagEntity tag2 =
        TagEntity.builder().withId(1L).withName("tag2").withAuditInfo(auditInfo).build();
    Assertions.assertNull(tag2.comment());
    Assertions.assertNull(tag2.properties());
  }

  @Test
  public void testHashCodeIncludesNamespace() {
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    TableEntity table1 =
        TableEntity.builder()
            .withId(1L)
            .withName("t")
            .withNamespace(Namespace.of("catalog", "schema1"))
            .withColumns(Collections.emptyList())
            .withAuditInfo(audit)
            .build();

    TableEntity table2 =
        TableEntity.builder()
            .withId(1L)
            .withName("t")
            .withNamespace(Namespace.of("catalog", "schema2"))
            .withColumns(Collections.emptyList())
            .withAuditInfo(audit)
            .build();

    Assertions.assertNotEquals(
        table1.hashCode(), table2.hashCode(), "hashCode should include namespace");
  }

  @Test
  public void testHashCodeWithDifferentNamespaceHavingSameValues() {
    AuditInfo audit = AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build();

    TableEntity table1 =
        TableEntity.builder()
            .withId(2L)
            .withName("t")
            .withNamespace(Namespace.of("catalog", "schema1"))
            .withColumns(Collections.emptyList())
            .withAuditInfo(audit)
            .build();

    TableEntity table2 =
        TableEntity.builder()
            .withId(2L)
            .withName("t")
            .withNamespace(Namespace.of("catalog", "schema1"))
            .withColumns(Collections.emptyList())
            .withAuditInfo(audit)
            .build();

    Assertions.assertEquals(table1.hashCode(), table2.hashCode(), "hashCode should be the same");
  }
}
