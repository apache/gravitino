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
package org.apache.gravitino.storage.relational.service;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;

public class TestFilesetMetaService extends TestJDBCBackend {
  private final String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("tst_fs_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("tst_fs_schema");

  @BeforeEach
  public void prepare() throws IOException {
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
  }

  @TestTemplate
  public void testInsertAlreadyExistsException() throws IOException {
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset",
            AUDIT_INFO);
    FilesetEntity filesetCopy =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(filesetCopy, false));
  }

  @TestTemplate
  public void testUpdateAlreadyExistsException() throws IOException {
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset",
            AUDIT_INFO);
    FilesetEntity filesetCopy =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset1",
            AUDIT_INFO);
    backend.insert(fileset, false);
    backend.insert(filesetCopy, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            backend.update(
                filesetCopy.nameIdentifier(),
                Entity.EntityType.FILESET,
                e ->
                    createFilesetEntity(
                        filesetCopy.id(), filesetCopy.namespace(), "fileset", AUDIT_INFO)));
  }

  @TestTemplate
  public void testMetaLifeCycleFromCreationToDeletion() throws IOException {
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset",
            AUDIT_INFO);
    backend.insert(fileset, false);

    // update fileset properties and version
    FilesetEntity filesetV2 =
        createFilesetEntity(
            fileset.id(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            "fileset",
            AUDIT_INFO);
    filesetV2.properties().put("version", "2");
    backend.update(fileset.nameIdentifier(), Entity.EntityType.FILESET, e -> filesetV2);

    String anotherMetalakeName = GravitinoITUtils.genRandomName("another-metalake");
    String anotherCatalogName = GravitinoITUtils.genRandomName("another-catalog");
    String anotherSchemaName = GravitinoITUtils.genRandomName("another-schema");
    createAndInsertMakeLake(anotherMetalakeName);
    createAndInsertCatalog(anotherMetalakeName, anotherCatalogName);
    createAndInsertSchema(anotherMetalakeName, anotherCatalogName, anotherSchemaName);

    FilesetEntity anotherFileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(anotherMetalakeName, anotherCatalogName, anotherSchemaName),
            "anotherFileset",
            AUDIT_INFO);
    backend.insert(anotherFileset, false);

    FilesetEntity anotherFilesetV2 =
        createFilesetEntity(
            anotherFileset.id(),
            NamespaceUtil.ofFileset(anotherMetalakeName, anotherCatalogName, anotherSchemaName),
            "anotherFileset",
            AUDIT_INFO);
    anotherFilesetV2.properties().put("version", "2");
    backend.update(
        anotherFileset.nameIdentifier(), Entity.EntityType.FILESET, e -> anotherFilesetV2);

    FilesetEntity anotherFilesetV3 =
        createFilesetEntity(
            anotherFileset.id(),
            NamespaceUtil.ofFileset(anotherMetalakeName, anotherCatalogName, anotherSchemaName),
            "anotherFileset",
            AUDIT_INFO);
    anotherFilesetV3.properties().put("version", "3");
    backend.update(
        anotherFileset.nameIdentifier(), Entity.EntityType.FILESET, e -> anotherFilesetV3);

    List<FilesetEntity> filesets =
        backend.list(fileset.namespace(), Entity.EntityType.FILESET, true);
    assertFalse(filesets.contains(fileset));
    assertTrue(filesets.contains(filesetV2));
    assertEquals("2", filesets.get(filesets.indexOf(filesetV2)).properties().get("version"));

    // meta data soft delete
    backend.delete(NameIdentifierUtil.ofMetalake(metalakeName), Entity.EntityType.METALAKE, true);
    assertFalse(backend.exists(fileset.nameIdentifier(), Entity.EntityType.FILESET));
    assertTrue(backend.exists(anotherFileset.nameIdentifier(), Entity.EntityType.FILESET));

    // check legacy record after soft delete
    assertTrue(legacyRecordExistsInDB(fileset.id(), Entity.EntityType.FILESET));
    assertEquals(2, listFilesetVersions(fileset.id()).size());
    assertEquals(3, listFilesetVersions(anotherFileset.id()).size());

    // meta data hard delete
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.hardDeleteLegacyData(entityType, Instant.now().toEpochMilli() + 1000);
    }
    assertFalse(legacyRecordExistsInDB(fileset.id(), Entity.EntityType.FILESET));
    assertEquals(0, listFilesetVersions(fileset.id()).size());
    assertEquals(3, listFilesetVersions(anotherFileset.id()).size());

    // soft delete for old version fileset
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.deleteOldVersionData(entityType, 1);
    }
    Map<Integer, Long> versionDeletedMap = listFilesetVersions(anotherFileset.id());
    assertEquals(3, versionDeletedMap.size());
    assertEquals(1, versionDeletedMap.values().stream().filter(value -> value == 0L).count());
    assertEquals(2, versionDeletedMap.values().stream().filter(value -> value != 0L).count());

    // hard delete for old version fileset
    backend.hardDeleteLegacyData(Entity.EntityType.FILESET, Instant.now().toEpochMilli() + 1000);
    assertEquals(1, listFilesetVersions(anotherFileset.id()).size());
  }

  @TestTemplate
  public void testFilesetMultipleLocations() {
    // test create
    String filesetName = GravitinoITUtils.genRandomName("multiple_location_fileset");
    NameIdentifier filesetIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName);
    String locationName = "location1";
    Map<String, String> locations =
        ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/tmp", locationName, "/tmp2");
    Namespace filesetNs = NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName);
    FilesetEntity filesetEntity =
        FilesetEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(filesetName)
            .withNamespace(filesetNs)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocations(locations)
            .withComment("")
            .withProperties(null)
            .withAuditInfo(AUDIT_INFO)
            .build();
    Assertions.assertDoesNotThrow(
        () -> FilesetMetaService.getInstance().insertFileset(filesetEntity, true));

    // test load
    FilesetEntity loadedFilesetEntity =
        FilesetMetaService.getInstance().getFilesetByIdentifier(filesetIdent);
    Assertions.assertEquals(filesetEntity, loadedFilesetEntity);

    // test update
    Map<String, String> newProps = ImmutableMap.of("k1", "v1", "k2", "v2");
    FilesetEntity updatedFilesetEntity =
        FilesetEntity.builder()
            .withId(loadedFilesetEntity.id())
            .withName(loadedFilesetEntity.name())
            .withNamespace(loadedFilesetEntity.namespace())
            .withFilesetType(loadedFilesetEntity.filesetType())
            .withStorageLocations(loadedFilesetEntity.storageLocations())
            .withComment(loadedFilesetEntity.comment())
            .withProperties(newProps)
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator2").withCreateTime(Instant.now()).build())
            .build();
    Assertions.assertDoesNotThrow(
        () ->
            FilesetMetaService.getInstance()
                .updateFileset(filesetIdent, e -> updatedFilesetEntity));
    FilesetEntity updatedLoadedFilesetEntity =
        FilesetMetaService.getInstance().getFilesetByIdentifier(filesetIdent);
    Assertions.assertEquals(updatedFilesetEntity, updatedLoadedFilesetEntity);

    // test list
    String filesetName2 = GravitinoITUtils.genRandomName("multiple_location_fileset2");
    NameIdentifier filesetIdent2 =
        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName2);
    FilesetEntity filesetEntity2 =
        FilesetEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(filesetName2)
            .withNamespace(filesetNs)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocations(locations)
            .withComment("")
            .withProperties(null)
            .withAuditInfo(AUDIT_INFO)
            .build();
    Assertions.assertDoesNotThrow(
        () -> FilesetMetaService.getInstance().insertFileset(filesetEntity2, true));
    int count = FilesetMetaService.getInstance().listFilesetsByNamespace(filesetNs).size();
    Assertions.assertEquals(2, count);

    // test delete
    Assertions.assertDoesNotThrow(
        () -> FilesetMetaService.getInstance().deleteFileset(filesetIdent2));
    Assertions.assertThrows(
        NoSuchEntityException.class,
        () -> FilesetMetaService.getInstance().getFilesetByIdentifier(filesetIdent2));
    List<Pair<Integer, String>> versionInfos = listFilesetInvalidVersions(filesetEntity2.id());
    Assertions.assertEquals(2, versionInfos.size());
    Assertions.assertEquals(1, versionInfos.get(0).getLeft());
    Set<String> locationNames =
        versionInfos.stream().map(Pair::getRight).collect(Collectors.toSet());
    Assertions.assertTrue(locationNames.contains(LOCATION_NAME_UNKNOWN));
    Assertions.assertTrue(locationNames.contains(locationName));
  }

  @TestTemplate
  public void testDeleteFilesetVersionsByRetentionCount() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_fileset");
    FilesetEntity filesetEntity =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp");
    FilesetMetaService.getInstance().insertFileset(filesetEntity, true);
    assertNotNull(
        FilesetMetaService.getInstance()
            .getFilesetByIdentifier(
                NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName)));
    FilesetMetaService.getInstance()
        .updateFileset(
            NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName),
            e -> {
              AuditInfo auditInfo1 =
                  AuditInfo.builder().withCreator("creator5").withCreateTime(Instant.now()).build();
              return createFilesetEntity(
                  filesetEntity.id(),
                  Namespace.of(metalakeName, catalogName, schemaName),
                  "filesetChanged",
                  auditInfo1,
                  "/tmp1");
            });
    FilesetMetaService.getInstance().deleteFilesetVersionsByRetentionCount(1L, 100);
    List<Pair<Integer, String>> versionInfos = listFilesetInvalidVersions(filesetEntity.id());
    // version 1 should be softly deleted
    Assertions.assertEquals(1, versionInfos.size());
    Assertions.assertEquals(1, versionInfos.get(0).getLeft());
    Assertions.assertEquals(LOCATION_NAME_UNKNOWN, versionInfos.get(0).getRight());
  }

  private List<Pair<Integer, String>> listFilesetInvalidVersions(Long filesetId) {
    List<Pair<Integer, String>> deletedVersions = Lists.newArrayList();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, storage_location_name FROM fileset_version_info WHERE fileset_id = %d and deleted_at > 0",
                    filesetId))) {
      while (rs.next()) {
        deletedVersions.add(Pair.of(rs.getInt("version"), rs.getString("storage_location_name")));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return deletedVersions;
  }

  private FilesetEntity createFilesetEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo, String location) {
    return FilesetEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withFilesetType(Fileset.Type.MANAGED)
        .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, location))
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }
}
