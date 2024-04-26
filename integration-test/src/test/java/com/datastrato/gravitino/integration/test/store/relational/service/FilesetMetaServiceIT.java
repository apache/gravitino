/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.integration.test.store.relational.service;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.config.ConfigConstants;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.integration.test.container.ContainerSuite;
import com.datastrato.gravitino.integration.test.container.MySQLContainer;
import com.datastrato.gravitino.integration.test.util.GravitinoITUtils;
import com.datastrato.gravitino.integration.test.util.TestDatabaseName;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.IdGenerator;
import com.datastrato.gravitino.storage.RandomIdGenerator;
import com.datastrato.gravitino.storage.relational.service.CatalogMetaService;
import com.datastrato.gravitino.storage.relational.service.FilesetMetaService;
import com.datastrato.gravitino.storage.relational.service.MetalakeMetaService;
import com.datastrato.gravitino.storage.relational.service.SchemaMetaService;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("gravitino-docker-it")
public class FilesetMetaServiceIT {
  private static final Logger LOG = LoggerFactory.getLogger(FilesetMetaServiceIT.class);
  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();

  @BeforeAll
  public static void setup() {
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
                          "/scripts/mysql/schema-%s-mysql.sql", ConfigConstants.VERSION_0_5_0)),
              "UTF-8");
      String[] initMySQLBackendSqls = mysqlContent.split(";");
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
  }

  @AfterAll
  public static void tearDown() {}

  @Test
  public void testDeleteFilesetVersionsByRetentionCount() throws IOException {
    IdGenerator idGenerator = new RandomIdGenerator();
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    String metalakeName = GravitinoITUtils.genRandomName("tst_metalake");
    BaseMetalake metalake = createBaseMakeLake(idGenerator.nextId(), metalakeName, auditInfo);
    MetalakeMetaService.getInstance().insertMetalake(metalake, true);
    assertNotNull(
        MetalakeMetaService.getInstance()
            .getMetalakeByIdentifier(NameIdentifier.ofMetalake(metalakeName)));
    String catalogName = GravitinoITUtils.genRandomName("tst_fs_catalog");
    CatalogEntity catalogEntity =
        createCatalog(
            idGenerator.nextId(), Namespace.ofCatalog(metalakeName), catalogName, auditInfo);
    CatalogMetaService.getInstance().insertCatalog(catalogEntity, true);
    assertNotNull(
        CatalogMetaService.getInstance()
            .getCatalogByIdentifier(NameIdentifier.ofCatalog(metalakeName, catalogName)));
    String schemaName = GravitinoITUtils.genRandomName("tst_fs_schema");
    SchemaEntity schemaEntity =
        createSchemaEntity(
            idGenerator.nextId(),
            Namespace.ofSchema(metalakeName, catalogName),
            schemaName,
            auditInfo);
    SchemaMetaService.getInstance().insertSchema(schemaEntity, true);
    assertNotNull(
        SchemaMetaService.getInstance()
            .getSchemaByIdentifier(NameIdentifier.ofSchema(metalakeName, catalogName, schemaName)));
    String filesetName = GravitinoITUtils.genRandomName("tst_fs_fileset");
    FilesetEntity filesetEntity =
        createFilesetEntity(
            idGenerator.nextId(),
            Namespace.ofFileset(metalakeName, catalogName, schemaName),
            filesetName,
            auditInfo,
            "/tmp");
    FilesetMetaService.getInstance().insertFileset(filesetEntity, true);
    assertNotNull(
        FilesetMetaService.getInstance()
            .getFilesetByIdentifier(
                NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName)));
    FilesetMetaService.getInstance()
        .updateFileset(
            NameIdentifier.ofFileset(metalakeName, catalogName, schemaName, filesetName),
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
    Map<Integer, Long> versionInfo = listFilesetValidVersions(filesetEntity.id());
    // version 1 should be softly deleted
    assertTrue(versionInfo.get(1) > 0);
  }

  private Map<Integer, Long> listFilesetValidVersions(Long filesetId) {
    Map<Integer, Long> versionDeletedTime = new HashMap<>();
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                String.format(
                    "SELECT version, deleted_at FROM fileset_version_info WHERE fileset_id = %d",
                    filesetId))) {
      while (rs.next()) {
        versionDeletedTime.put(rs.getInt("version"), rs.getLong("deleted_at"));
      }
    } catch (SQLException e) {
      throw new RuntimeException("SQL execution failed", e);
    }
    return versionDeletedTime;
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
        .withStorageLocation(location)
        .withComment("")
        .withProperties(null)
        .withAuditInfo(auditInfo)
        .build();
  }
}
