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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.config.ConfigConstants;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.MySQLContainer;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.integration.test.util.TestDatabaseName;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.SchemaVersion;
import org.apache.gravitino.storage.IdGenerator;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;

@Tag("gravitino-docker-test")
@EnabledIfEnvironmentVariable(named = "jdbcBackend", matches = "MySQL")
public class TestFilesetMetaService {
  private static final Logger LOG = LoggerFactory.getLogger(TestFilesetMetaService.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static final IdGenerator idGenerator = new RandomIdGenerator();

  private static final AuditInfo auditInfo =
      AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  private static final String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
  private static final String catalogName = GravitinoITUtils.genRandomName("tst_fs_catalog");
  private static final String schemaName = GravitinoITUtils.genRandomName("tst_fs_schema");

  @BeforeAll
  public static void setup() throws IOException {
    TestDatabaseName META_DATA = TestDatabaseName.MYSQL_JDBC_BACKEND;
    containerSuite.startMySQLContainer(META_DATA);
    MySQLContainer MYSQL_CONTAINER = containerSuite.getMySQLContainer();

    String mysqlUrl = MYSQL_CONTAINER.getJdbcUrl(META_DATA);
    LOG.info("MySQL URL: {}", mysqlUrl);
    // Connect to the mysql docker and create a databases
    try (Connection connection =
            DriverManager.getConnection(
                StringUtils.substring(mysqlUrl, 0, mysqlUrl.lastIndexOf("/")), "root", "root");
        final Statement statement = connection.createStatement()) {
      statement.execute("drop database if exists " + META_DATA);
      statement.execute("create database " + META_DATA);
      String gravitinoHome = System.getenv("GRAVITINO_ROOT_DIR");
      String mysqlContent =
          FileUtils.readFileToString(
              new File(
                  gravitinoHome
                      + String.format(
                          "/scripts/mysql/schema-%s-mysql.sql",
                          ConfigConstants.CURRENT_SCRIPT_VERSION)),
              "UTF-8");
      String[] initMySQLBackendSqls =
          Arrays.stream(mysqlContent.split(";"))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .toArray(String[]::new);
      initMySQLBackendSqls = ArrayUtils.addFirst(initMySQLBackendSqls, "use " + META_DATA + ";");
      for (String sql : initMySQLBackendSqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      LOG.error("Failed to create database in mysql", e);
      throw new RuntimeException(e);
    }

    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL)).thenReturn(mysqlUrl);
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER))
        .thenReturn("com.mysql.cj.jdbc.Driver");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("root");

    SqlSessionFactoryHelper.getInstance().init(config);

    prepareMetalake();
    prepareCatalog();
    prepareSchema();
  }

  private static void prepareSchema() throws IOException {
    SchemaEntity schemaEntity =
        createSchemaEntity(
            idGenerator.nextId(),
            NamespaceUtil.ofSchema(metalakeName, catalogName),
            schemaName,
            auditInfo);
    SchemaMetaService.getInstance().insertSchema(schemaEntity, true);
    assertNotNull(
        SchemaMetaService.getInstance()
            .getSchemaByIdentifier(NameIdentifier.of(metalakeName, catalogName, schemaName)));
  }

  private static void prepareMetalake() throws IOException {
    BaseMetalake metalake = createBaseMakeLake(idGenerator.nextId(), metalakeName, auditInfo);
    MetalakeMetaService.getInstance().insertMetalake(metalake, true);
    assertNotNull(
        MetalakeMetaService.getInstance().getMetalakeByIdentifier(NameIdentifier.of(metalakeName)));
  }

  private static void prepareCatalog() throws IOException {
    CatalogEntity catalogEntity =
        createCatalog(
            idGenerator.nextId(), NamespaceUtil.ofCatalog(metalakeName), catalogName, auditInfo);
    CatalogMetaService.getInstance().insertCatalog(catalogEntity, true);
    assertNotNull(
        CatalogMetaService.getInstance()
            .getCatalogByIdentifier(NameIdentifier.of(metalakeName, catalogName)));
  }

  @AfterEach
  public void cleanFilesets() throws IOException {
    SchemaMetaService.getInstance()
        .deleteSchema(NameIdentifier.of(metalakeName, catalogName, schemaName), true);
    prepareSchema();
  }

  @Test
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
            .withId(idGenerator.nextId())
            .withName(filesetName)
            .withNamespace(filesetNs)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocations(locations)
            .withComment("")
            .withProperties(null)
            .withAuditInfo(auditInfo)
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
            .withId(idGenerator.nextId())
            .withName(filesetName2)
            .withNamespace(filesetNs)
            .withFilesetType(Fileset.Type.MANAGED)
            .withStorageLocations(locations)
            .withComment("")
            .withProperties(null)
            .withAuditInfo(auditInfo)
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

  @Test
  public void testDeleteFilesetVersionsByRetentionCount() throws IOException {
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_fileset");
    FilesetEntity filesetEntity =
        createFilesetEntity(
            idGenerator.nextId(),
            NamespaceUtil.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            auditInfo,
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

  public static BaseMetalake createBaseMakeLake(Long id, String name, AuditInfo auditInfo) {
    return BaseMetalake.builder()
        .withId(id)
        .withName(name)
        .withAuditInfo(auditInfo)
        .withComment("")
        .withProperties(null)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  public static CatalogEntity createCatalog(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static SchemaEntity createSchemaEntity(
      Long id, Namespace namespace, String name, AuditInfo auditInfo) {
    return SchemaEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }

  public static FilesetEntity createFilesetEntity(
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
