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

import com.google.common.collect.ImmutableMap;
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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.exceptions.OptimisticLockException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.TestJDBCBackend;
import org.apache.gravitino.storage.relational.mapper.FilesetMetaMapper;
import org.apache.gravitino.storage.relational.mapper.FilesetVersionMapper;
import org.apache.gravitino.storage.relational.po.FilesetPO;
import org.apache.gravitino.storage.relational.po.FilesetVersionPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.Mockito;

public class TestFilesetMetaService extends TestJDBCBackend {
  private final String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
  private final String catalogName = GravitinoITUtils.genRandomName("tst_fs_catalog");
  private final String schemaName = GravitinoITUtils.genRandomName("tst_fs_schema");

  @BeforeEach
  public void prepare() throws IOException, IllegalAccessException {
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.CACHE_ENABLED)).thenReturn(false);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
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
    Map<Integer, Long> anotherFilesetVersionsAfterHardDelete =
        listFilesetVersions(anotherFileset.id());
    assertTrue(anotherFilesetVersionsAfterHardDelete.containsKey(3));
    assertEquals(0L, anotherFilesetVersionsAfterHardDelete.get(3));

    // soft delete for old version fileset
    for (Entity.EntityType entityType : Entity.EntityType.values()) {
      backend.deleteOldVersionData(entityType, 1);
    }
    Map<Integer, Long> versionDeletedMap = listFilesetVersions(anotherFileset.id());
    assertTrue(versionDeletedMap.containsKey(3));
    assertEquals(0L, versionDeletedMap.get(3));
    assertEquals(1, versionDeletedMap.values().stream().filter(value -> value == 0L).count());

    // hard delete for old version fileset
    backend.hardDeleteLegacyData(Entity.EntityType.FILESET, Instant.now().toEpochMilli() + 1000);
    Map<Integer, Long> finalFilesetVersions = listFilesetVersions(anotherFileset.id());
    assertTrue(finalFilesetVersions.containsKey(3));
    assertEquals(0L, finalFilesetVersions.get(3));
    assertEquals(1, finalFilesetVersions.values().stream().filter(value -> value == 0L).count());
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
    Map<Integer, Long> versionDeletedMap = listFilesetVersions(filesetEntity.id());
    assertEquals(2, versionDeletedMap.size());
    assertVersionActive(versionDeletedMap, 1);
    assertVersionActive(versionDeletedMap, 2);

    FilesetMetaService.getInstance().deleteFilesetVersionsByRetentionCount(1L, 100);
    versionDeletedMap = listFilesetVersions(filesetEntity.id());
    assertEquals(2, versionDeletedMap.size());
    assertVersionSoftDeleted(versionDeletedMap, 1);
    assertVersionActive(versionDeletedMap, 2);
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

  private void assertVersionActive(Map<Integer, Long> versionDeletedMap, int version) {
    assertTrue(versionDeletedMap.containsKey(version));
    assertEquals(0L, versionDeletedMap.get(version));
  }

  private void assertVersionSoftDeleted(Map<Integer, Long> versionDeletedMap, int version) {
    assertTrue(versionDeletedMap.containsKey(version));
    assertTrue(versionDeletedMap.get(version) > 0L);
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

  @TestTemplate
  public void testAlterReportsOptimisticLockConflictAndKeepsWinnerVersion() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_conflict");
    NameIdentifier filesetIdent =
        NameIdentifier.of(metalakeName, catalogName, schemaName, filesetName);
    FilesetEntity filesetEntity =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp");
    FilesetMetaService.getInstance().insertFileset(filesetEntity, true);
    FilesetPO initialPO = getFilesetPO(filesetEntity.id());

    AuditInfo conflictingAuditInfo =
        AuditInfo.builder()
            .withCreator("conflicting-updater")
            .withCreateTime(Instant.now())
            .build();
    FilesetEntity updatedFilesetEntity =
        FilesetEntity.builder()
            .withId(filesetEntity.id())
            .withName(filesetEntity.name())
            .withNamespace(filesetEntity.namespace())
            .withFilesetType(filesetEntity.filesetType())
            .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/tmp-v2"))
            .withComment("comment-v2")
            .withProperties(ImmutableMap.of("version", "2"))
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator("expected-updater")
                    .withCreateTime(Instant.now())
                    .build())
            .build();

    Assertions.assertThrows(
        OptimisticLockException.class,
        () ->
            FilesetMetaService.getInstance()
                .updateFileset(
                    filesetIdent,
                    e -> {
                      // Commit another alter after the outer call has read its snapshot. The
                      // outer write must then lose the current_version comparison.
                      updateFilesetUnchecked(
                          filesetIdent,
                          entity ->
                              createFilesetEntity(
                                  entity.id(),
                                  entity.namespace(),
                                  entity.name(),
                                  conflictingAuditInfo,
                                  "/tmp"));
                      return updatedFilesetEntity;
                    }));

    FilesetEntity persistedEntity =
        FilesetMetaService.getInstance().getFilesetByIdentifier(filesetIdent);
    Assertions.assertEquals(conflictingAuditInfo, persistedEntity.auditInfo());
    Assertions.assertEquals("", persistedEntity.comment());
    Assertions.assertNull(persistedEntity.properties());
    Assertions.assertEquals("/tmp", persistedEntity.storageLocations().get(LOCATION_NAME_UNKNOWN));
    Assertions.assertNotEquals(updatedFilesetEntity, persistedEntity);
    FilesetPO currentPO = getFilesetPO(filesetEntity.id());
    Assertions.assertEquals(
        initialPO.getCurrentVersion() + 1, currentPO.getCurrentVersion().longValue());
    Assertions.assertEquals(currentPO.getCurrentVersion(), currentPO.getLastVersion());
    Assertions.assertEquals(2, listFilesetVersions(filesetEntity.id()).size());
  }

  @TestTemplate
  public void testOverwriteAdvancesVersionAndRejectsStaleAlter() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_overwrite_occ");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp-original");
    FilesetMetaService.getInstance().insertFileset(original, false);
    FilesetPO beforeOverwrite = getFilesetPO(original.id());
    FilesetEntity replacement =
        copyFileset(
            original,
            original.id(),
            original.name(),
            "overwrite winner",
            "/tmp-overwrite",
            original.auditInfo());

    assertThrows(
        OptimisticLockException.class,
        () ->
            FilesetMetaService.getInstance()
                .updateFileset(
                    original.nameIdentifier(),
                    entity -> {
                      insertFilesetUnchecked(replacement, true);
                      FilesetEntity current = (FilesetEntity) entity;
                      return copyFileset(
                          current,
                          current.id(),
                          current.name(),
                          "stale alter",
                          "/tmp-stale",
                          current.auditInfo());
                    }));

    FilesetEntity winner =
        FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier());
    FilesetPO afterOverwrite = getFilesetPO(original.id());
    Assertions.assertEquals("overwrite winner", winner.comment());
    Assertions.assertEquals("/tmp-overwrite", winner.storageLocations().get(LOCATION_NAME_UNKNOWN));
    Assertions.assertEquals(
        beforeOverwrite.getCurrentVersion() + 1, afterOverwrite.getCurrentVersion().longValue());
    Assertions.assertEquals(afterOverwrite.getCurrentVersion(), afterOverwrite.getLastVersion());
  }

  @TestTemplate
  public void testNaturalKeyOverwriteUsesPersistedFilesetId() throws IOException {
    // PostgreSQL targets fileset_id explicitly and rejects a different ID on the natural key.
    // MySQL/H2 may resolve ON DUPLICATE KEY through either key and must preserve the stored ID.
    Assumptions.assumeFalse("postgresql".equalsIgnoreCase(backendType));
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_natural_key_overwrite");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp-original");
    FilesetMetaService.getInstance().insertFileset(original, false);
    FilesetPO beforeOverwrite = getFilesetPO(original.id());
    Map<String, String> replacementLocations =
        ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/tmp-replacement", "archive", "/tmp-archive");
    FilesetEntity replacement =
        FilesetEntity.builder()
            .withId(RandomIdGenerator.INSTANCE.nextId())
            .withName(original.name())
            .withNamespace(original.namespace())
            .withFilesetType(original.filesetType())
            .withStorageLocations(replacementLocations)
            .withComment("replacement")
            .withProperties(original.properties())
            .withAuditInfo(original.auditInfo())
            .build();

    FilesetMetaService.getInstance().insertFileset(replacement, true);

    FilesetEntity stored =
        FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier());
    FilesetPO afterOverwrite = getFilesetPO(original.id());
    Assertions.assertEquals(original.id(), stored.id());
    Assertions.assertEquals("replacement", stored.comment());
    Assertions.assertEquals(replacementLocations, stored.storageLocations());
    Assertions.assertEquals(
        beforeOverwrite.getCurrentVersion() + 1, afterOverwrite.getCurrentVersion().longValue());
  }

  @TestTemplate
  public void testAlterReportsNoSuchWhenRenamedConcurrently() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_rename_conflict");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp");
    FilesetMetaService.getInstance().insertFileset(original, false);
    String renamedName = filesetName + "_winner";
    NameIdentifier renamedIdentifier = NameIdentifier.of(original.namespace(), renamedName);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            FilesetMetaService.getInstance()
                .updateFileset(
                    original.nameIdentifier(),
                    entity -> {
                      updateFilesetUnchecked(
                          original.nameIdentifier(),
                          current ->
                              copyFileset(
                                  current,
                                  current.id(),
                                  renamedName,
                                  "rename winner",
                                  "/tmp",
                                  current.auditInfo()));
                      FilesetEntity current = (FilesetEntity) entity;
                      return copyFileset(
                          current,
                          current.id(),
                          current.name(),
                          "stale alter",
                          "/tmp",
                          current.auditInfo());
                    }));

    assertThrows(
        NoSuchEntityException.class,
        () -> FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier()));
    Assertions.assertEquals(
        "rename winner",
        FilesetMetaService.getInstance().getFilesetByIdentifier(renamedIdentifier).comment());
  }

  @TestTemplate
  public void testDeleteRejectsStaleVersionAndKeepsVersions() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_stale_delete");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp-v1");
    FilesetMetaService.getInstance().insertFileset(original, false);
    FilesetPO stalePO = getFilesetPO(original.id());

    FilesetMetaService.getInstance()
        .updateFileset(
            original.nameIdentifier(),
            entity -> {
              FilesetEntity current = (FilesetEntity) entity;
              return copyFileset(
                  current,
                  current.id(),
                  current.name(),
                  "winning alter",
                  "/tmp-v2",
                  current.auditInfo());
            });

    assertThrows(
        OptimisticLockException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    FilesetMetaService.getInstance()
                        .deleteFilesetWithVersion(original.nameIdentifier(), stalePO)));

    FilesetEntity current =
        FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier());
    Assertions.assertEquals("winning alter", current.comment());
    Assertions.assertEquals("/tmp-v2", current.storageLocations().get(LOCATION_NAME_UNKNOWN));
    Map<Integer, Long> versions = listFilesetVersions(original.id());
    Assertions.assertEquals(2, versions.size());
    assertVersionActive(versions, 1);
    assertVersionActive(versions, 2);
  }

  @TestTemplate
  public void testDeleteReportsNoSuchWhenDeletedConcurrently() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_double_delete");
    FilesetEntity fileset =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp");
    FilesetMetaService.getInstance().insertFileset(fileset, false);
    FilesetPO stalePO = getFilesetPO(fileset.id());

    FilesetMetaService.getInstance().deleteFileset(fileset.nameIdentifier());

    assertThrows(
        NoSuchEntityException.class,
        () ->
            SessionUtils.doMultipleWithCommit(
                () ->
                    FilesetMetaService.getInstance()
                        .deleteFilesetWithVersion(fileset.nameIdentifier(), stalePO)));
  }

  @TestTemplate
  public void testUpdateRollsBackMetadataWhenVersionInsertFails() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_update_rollback");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp-v1");
    FilesetMetaService.getInstance().insertFileset(original, false);
    FilesetPO initialPO = getFilesetPO(original.id());
    // fileset_meta carries no comment, so an over-long comment passes the metadata update and only
    // fails once the version snapshot is written.
    String tooLongComment = StringUtils.repeat("c", 300);

    // Each backend reports the rejected snapshot differently, so only the rollback below is
    // asserted on.
    assertThrows(
        Exception.class,
        () ->
            FilesetMetaService.getInstance()
                .updateFileset(
                    original.nameIdentifier(),
                    entity -> {
                      FilesetEntity current = (FilesetEntity) entity;
                      return copyFileset(
                          current,
                          current.id(),
                          current.name(),
                          tooLongComment,
                          "/tmp-v2",
                          current.auditInfo());
                    }));

    FilesetEntity current =
        FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier());
    FilesetPO currentPO = getFilesetPO(original.id());
    Assertions.assertEquals(original.comment(), current.comment());
    Assertions.assertEquals(
        original.storageLocations().get(LOCATION_NAME_UNKNOWN),
        current.storageLocations().get(LOCATION_NAME_UNKNOWN));
    Assertions.assertEquals(initialPO.getCurrentVersion(), currentPO.getCurrentVersion());
    Assertions.assertEquals(initialPO.getLastVersion(), currentPO.getLastVersion());
  }

  @TestTemplate
  public void testAlterSkipsVersionsAlreadyStored() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_stale_version");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp-v1");
    FilesetMetaService.getInstance().insertFileset(original, false);
    FilesetPO initialPO = getFilesetPO(original.id());

    // A fileset written before the version reset was fixed owns snapshots above the version its
    // metadata row records. The next alter has to start above those, not on top of them.
    FilesetVersionPO staleVersion =
        FilesetVersionPO.builder()
            .withMetalakeId(initialPO.getMetalakeId())
            .withCatalogId(initialPO.getCatalogId())
            .withSchemaId(initialPO.getSchemaId())
            .withFilesetId(initialPO.getFilesetId())
            .withVersion(initialPO.getCurrentVersion() + 1)
            .withFilesetComment("left behind by an older release")
            .withLocationName(LOCATION_NAME_UNKNOWN)
            .withStorageLocation("/tmp-stale")
            .withDeletedAt(0L)
            .build();
    SessionUtils.doWithCommit(
        FilesetVersionMapper.class, mapper -> mapper.insertFilesetVersions(List.of(staleVersion)));

    FilesetEntity altered =
        FilesetMetaService.getInstance()
            .updateFileset(
                original.nameIdentifier(),
                entity -> {
                  FilesetEntity current = (FilesetEntity) entity;
                  return copyFileset(
                      current,
                      current.id(),
                      current.name(),
                      "altered past the stale version",
                      "/tmp-v2",
                      current.auditInfo());
                });

    Assertions.assertEquals("altered past the stale version", altered.comment());
    FilesetPO afterAlter = getFilesetPO(original.id());
    Assertions.assertEquals(
        staleVersion.getVersion() + 1, afterAlter.getCurrentVersion().longValue());
    FilesetEntity reloaded =
        FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier());
    Assertions.assertEquals("altered past the stale version", reloaded.comment());
    Assertions.assertEquals("/tmp-v2", reloaded.storageLocations().get(LOCATION_NAME_UNKNOWN));
  }

  @TestTemplate
  public void testOverwriteSkipsVersionsAlreadyStored() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_overwrite_stale_version");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp-v1");
    FilesetMetaService.getInstance().insertFileset(original, false);
    FilesetPO initialPO = getFilesetPO(original.id());

    // The same legacy shape the alter path already handles: a snapshot above the version the
    // metadata row records. The overwrite derives its version from that row, so it has to be
    // lifted above the snapshot instead of rewriting it.
    FilesetVersionPO staleVersion =
        FilesetVersionPO.builder()
            .withMetalakeId(initialPO.getMetalakeId())
            .withCatalogId(initialPO.getCatalogId())
            .withSchemaId(initialPO.getSchemaId())
            .withFilesetId(initialPO.getFilesetId())
            .withVersion(initialPO.getCurrentVersion() + 1)
            .withFilesetComment("left behind by an older release")
            .withLocationName(LOCATION_NAME_UNKNOWN)
            .withStorageLocation("/tmp-stale")
            .withDeletedAt(0L)
            .build();
    SessionUtils.doWithCommit(
        FilesetVersionMapper.class, mapper -> mapper.insertFilesetVersions(List.of(staleVersion)));

    FilesetEntity replacement =
        copyFileset(
            original,
            original.id(),
            original.name(),
            "overwritten past the stale version",
            "/tmp-v2",
            original.auditInfo());
    FilesetMetaService.getInstance().insertFileset(replacement, true);

    FilesetPO afterOverwrite = getFilesetPO(original.id());
    Assertions.assertEquals(
        staleVersion.getVersion() + 1, afterOverwrite.getCurrentVersion().longValue());
    FilesetEntity stored =
        FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier());
    Assertions.assertEquals("overwritten past the stale version", stored.comment());
    Assertions.assertEquals("/tmp-v2", stored.storageLocations().get(LOCATION_NAME_UNKNOWN));
    // The snapshot that was left behind is untouched at its own version.
    Assertions.assertEquals(
        "/tmp-stale",
        storageLocationOfVersion(initialPO.getFilesetId(), staleVersion.getVersion()));
  }

  @TestTemplate
  public void testNaturalKeyOverwriteRewritesIdentifierProperty() throws IOException {
    // PostgreSQL targets fileset_id explicitly and rejects a different ID on the natural key.
    Assumptions.assumeFalse("postgresql".equalsIgnoreCase(backendType));
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_overwrite_identifier");
    FilesetEntity original =
        createFilesetEntity(
            RandomIdGenerator.INSTANCE.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            AUDIT_INFO,
            "/tmp-original");
    FilesetMetaService.getInstance().insertFileset(original, false);

    long replacementId = RandomIdGenerator.INSTANCE.nextId();
    FilesetEntity replacement =
        FilesetEntity.builder()
            .withId(replacementId)
            .withName(original.name())
            .withNamespace(original.namespace())
            .withFilesetType(original.filesetType())
            .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "/tmp-replacement"))
            .withComment("replacement")
            .withProperties(
                ImmutableMap.of(
                    StringIdentifier.ID_KEY, StringIdentifier.fromId(replacementId).toString()))
            .withAuditInfo(original.auditInfo())
            .build();

    FilesetMetaService.getInstance().insertFileset(replacement, true);

    // The overwrite keeps the fileset ID the database already had, so the identifier property has
    // to name that ID as well instead of the one the rejected snapshot was built with.
    FilesetEntity stored =
        FilesetMetaService.getInstance().getFilesetByIdentifier(original.nameIdentifier());
    Assertions.assertEquals(original.id(), stored.id());
    Assertions.assertEquals(
        StringIdentifier.fromId(original.id()).toString(),
        stored.properties().get(StringIdentifier.ID_KEY));
  }

  private String storageLocationOfVersion(Long filesetId, Long version) {
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT storage_location FROM fileset_version_info"
                        + " WHERE fileset_id = %d AND version = %d AND deleted_at = 0",
                    filesetId, version))) {
      return rs.next() ? rs.getString("storage_location") : null;
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
  }

  private FilesetPO getFilesetPO(Long filesetId) {
    return SessionUtils.getWithoutCommit(
        FilesetMetaMapper.class, mapper -> mapper.selectFilesetMetaById(filesetId));
  }

  private FilesetEntity copyFileset(
      FilesetEntity source,
      Long id,
      String name,
      String comment,
      String location,
      AuditInfo auditInfo) {
    return FilesetEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(source.namespace())
        .withFilesetType(source.filesetType())
        .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, location))
        .withComment(comment)
        .withProperties(source.properties())
        .withAuditInfo(auditInfo)
        .build();
  }

  private void updateFilesetUnchecked(
      NameIdentifier identifier, Function<FilesetEntity, FilesetEntity> updater) {
    try {
      FilesetMetaService.getInstance().updateFileset(identifier, updater);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void insertFilesetUnchecked(FilesetEntity fileset, boolean overwrite) {
    try {
      FilesetMetaService.getInstance().insertFileset(fileset, overwrite);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
