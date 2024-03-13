/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.exceptions.NonEmptyEntityException;
import com.datastrato.gravitino.file.Fileset;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRelationalEntityStore {
  private static final Logger Logger = LoggerFactory.getLogger(TestRelationalEntityStore.class);
  private static final String MYSQL_STORE_PATH =
      "/tmp/gravitino_test_entityStore_" + UUID.randomUUID().toString().replace("-", "");
  private static final String DB_DIR = MYSQL_STORE_PATH + "/testdb";
  private static EntityStore entityStore = null;

  @BeforeAll
  public static void setUp() {
    File dir = new File(DB_DIR);
    if (dir.exists() || !dir.isDirectory()) {
      dir.delete();
    }
    dir.mkdirs();

    // Use H2 DATABASE to simulate MySQL
    Config config = Mockito.mock(Config.class);
    Mockito.when(config.get(ENTITY_STORE)).thenReturn(RELATIONAL_ENTITY_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_STORE)).thenReturn(DEFAULT_ENTITY_RELATIONAL_STORE);
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_USER)).thenReturn("root");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD)).thenReturn("123");
    Mockito.when(config.get(ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
    entityStore = EntityStoreFactory.createEntityStore(config);
    entityStore.initialize(config);

    // Read the ddl sql to create table
    String scriptPath = "h2/h2-init.sql";
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        Statement statement = connection.createStatement()) {
      URL scriptUrl = ClassLoader.getSystemResource(scriptPath);
      if (scriptUrl == null) {
        throw new IllegalStateException("Cannot find init sql script:" + scriptPath);
      }
      StringBuilder ddlBuilder = new StringBuilder();
      try (InputStreamReader inputStreamReader =
              new InputStreamReader(
                  Files.newInputStream(Paths.get(scriptUrl.getPath())), StandardCharsets.UTF_8);
          BufferedReader bufferedReader = new BufferedReader(inputStreamReader)) {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
          ddlBuilder.append(line).append("\n");
        }
      }
      statement.execute(ddlBuilder.toString());
    } catch (Exception e) {
      throw new IllegalStateException("Create tables failed", e);
    }
  }

  @AfterEach
  public void destroy() {
    truncateAllTables();
  }

  @AfterAll
  public static void tearDown() {
    dropAllTables();
    try {
      entityStore.close();
    } catch (IOException e) {
      Logger.error("Close the entity store failed:", e);
    }

    File dir = new File(DB_DIR);
    if (dir.exists()) {
      dir.delete();
    }
  }

  @Test
  public void testMetalakePutAndGet() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);
    BaseMetalake insertedMetalake =
        entityStore.get(metalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class);
    assertNotNull(insertedMetalake);
    assertTrue(checkMetalakeEquals(metalake, insertedMetalake));

    // overwrite false
    BaseMetalake duplicateMetalake = createMetalake(1L, "test_metalake", "this is test");
    assertThrows(
        EntityAlreadyExistsException.class, () -> entityStore.put(duplicateMetalake, false));

    // overwrite true
    BaseMetalake overwrittenMetalake = createMetalake(1L, "test_metalake2", "this is test2");
    entityStore.put(overwrittenMetalake, true);
    BaseMetalake insertedMetalake1 =
        entityStore.get(
            overwrittenMetalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class);
    assertEquals(
        1,
        entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE).size());
    assertEquals(overwrittenMetalake.name(), insertedMetalake1.name());
    assertEquals(overwrittenMetalake.comment(), insertedMetalake1.comment());
  }

  @Test
  public void testCatalogPutAndGet() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is catalog test");
    entityStore.put(catalog, false);

    CatalogEntity insertedCatalog =
        entityStore.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class);
    assertNotNull(insertedCatalog);
    assertTrue(checkCatalogEquals(catalog, insertedCatalog));

    // overwrite false
    CatalogEntity duplicateCatalog =
        createCatalog(
            1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is catalog test");
    assertThrows(
        EntityAlreadyExistsException.class, () -> entityStore.put(duplicateCatalog, false));

    // overwrite true
    CatalogEntity overwrittenCatalog =
        createCatalog(
            1L, "test_catalog1", Namespace.ofCatalog(metalake.name()), "this is catalog test1");
    entityStore.put(overwrittenCatalog, true);
    CatalogEntity insertedCatalog1 =
        entityStore.get(
            overwrittenCatalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class);
    assertEquals(
        1,
        entityStore
            .list(overwrittenCatalog.namespace(), CatalogEntity.class, Entity.EntityType.CATALOG)
            .size());
    assertEquals(overwrittenCatalog.name(), insertedCatalog1.name());
    assertEquals(overwrittenCatalog.getComment(), insertedCatalog1.getComment());
  }

  @Test
  public void testSchemaPutAndGet() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test");
    entityStore.put(schema, false);

    SchemaEntity insertedSchema =
        entityStore.get(schema.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class);
    assertNotNull(insertedSchema);
    assertTrue(checkSchemaEquals(schema, insertedSchema));

    // overwrite false
    SchemaEntity duplicateSchema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test");
    assertThrows(EntityAlreadyExistsException.class, () -> entityStore.put(duplicateSchema, false));

    // overwrite true
    SchemaEntity overwrittenSchema =
        createSchema(
            1L,
            "test_schema1",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test1");
    entityStore.put(overwrittenSchema, true);
    SchemaEntity insertedSchema1 =
        entityStore.get(
            overwrittenSchema.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class);

    assertEquals(
        1,
        entityStore
            .list(insertedSchema1.namespace(), SchemaEntity.class, Entity.EntityType.SCHEMA)
            .size());
    assertEquals(overwrittenSchema.name(), insertedSchema1.name());
    assertEquals(overwrittenSchema.comment(), insertedSchema1.comment());
  }

  @Test
  public void testTablePutAndGet() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test");
    entityStore.put(schema, false);

    TableEntity table =
        createTable(
            1L, "test_table", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    entityStore.put(table, false);

    TableEntity insertedTable =
        entityStore.get(table.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class);
    assertNotNull(insertedTable);
    assertTrue(checkTableEquals(table, insertedTable));

    // overwrite false
    TableEntity duplicateTable =
        createTable(
            1L, "test_table", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    assertThrows(EntityAlreadyExistsException.class, () -> entityStore.put(duplicateTable, false));

    // overwrite true
    TableEntity overwrittenTable =
        createTable(
            1L, "test_table1", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    entityStore.put(overwrittenTable, true);
    TableEntity insertedTable1 =
        entityStore.get(
            overwrittenTable.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class);

    assertEquals(
        1,
        entityStore
            .list(insertedTable1.namespace(), TableEntity.class, Entity.EntityType.TABLE)
            .size());
    assertEquals(overwrittenTable.name(), insertedTable1.name());
    assertEquals(overwrittenTable.auditInfo().creator(), insertedTable1.auditInfo().creator());
  }

  @Test
  public void testFilesetPutAndGet() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test");
    entityStore.put(schema, false);

    FilesetEntity filesetEntity =
        createFileset(
            1L,
            "test_fileset",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset test",
            "hdfs://localhost/test");
    entityStore.put(filesetEntity, false);

    FilesetEntity insertedFileset =
        entityStore.get(
            filesetEntity.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class);
    assertNotNull(insertedFileset);
    assertTrue(checkFilesetEquals(filesetEntity, insertedFileset));

    // overwrite false
    FilesetEntity duplicateFileset =
        createFileset(
            1L,
            "test_fileset",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset test",
            "hdfs://localhost/test");
    assertThrows(
        EntityAlreadyExistsException.class, () -> entityStore.put(duplicateFileset, false));

    // overwrite true
    FilesetEntity overwrittenFileset =
        createFileset(
            1L,
            "test_fileset1",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset test1",
            "hdfs://localhost/test1");
    entityStore.put(overwrittenFileset, true);
    FilesetEntity insertedFileset1 =
        entityStore.get(
            overwrittenFileset.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class);

    assertEquals(
        1,
        entityStore
            .list(insertedFileset1.namespace(), FilesetEntity.class, Entity.EntityType.FILESET)
            .size());
    checkFilesetEquals(overwrittenFileset, insertedFileset1);
  }

  @Test
  public void testMetalakePutAndList() throws IOException {
    BaseMetalake metalake1 = createMetalake(1L, "test_metalake1", "this is test 1");
    BaseMetalake metalake2 = createMetalake(2L, "test_metalake2", "this is test 2");
    List<BaseMetalake> beforePutList =
        entityStore.list(metalake1.namespace(), BaseMetalake.class, Entity.EntityType.METALAKE);
    assertNotNull(beforePutList);
    assertEquals(0, beforePutList.size());

    entityStore.put(metalake1, false);
    entityStore.put(metalake2, false);
    List<BaseMetalake> metalakes =
        entityStore.list(metalake1.namespace(), BaseMetalake.class, Entity.EntityType.METALAKE)
            .stream()
            .sorted(Comparator.comparing(BaseMetalake::id))
            .collect(Collectors.toList());
    assertNotNull(metalakes);
    assertEquals(2, metalakes.size());
    assertTrue(checkMetalakeEquals(metalake1, metalakes.get(0)));
    assertTrue(checkMetalakeEquals(metalake2, metalakes.get(1)));
  }

  @Test
  public void testCatalogPutAndList() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test 1");
    entityStore.put(metalake, false);

    CatalogEntity catalog1 =
        createCatalog(
            1L, "test_catalog1", Namespace.ofCatalog(metalake.name()), "this is catalog 1");
    CatalogEntity catalog2 =
        createCatalog(
            2L, "test_catalog2", Namespace.ofCatalog(metalake.name()), "this is catalog 2");
    List<CatalogEntity> beforeCatalogList =
        entityStore.list(catalog1.namespace(), CatalogEntity.class, Entity.EntityType.CATALOG);
    assertNotNull(beforeCatalogList);
    assertEquals(0, beforeCatalogList.size());

    entityStore.put(catalog1, false);
    entityStore.put(catalog2, false);
    List<CatalogEntity> catalogEntities =
        entityStore.list(catalog1.namespace(), CatalogEntity.class, Entity.EntityType.CATALOG)
            .stream()
            .sorted(Comparator.comparing(CatalogEntity::id))
            .collect(Collectors.toList());
    assertNotNull(catalogEntities);
    assertEquals(2, catalogEntities.size());
    assertTrue(checkCatalogEquals(catalog1, catalogEntities.get(0)));
    assertTrue(checkCatalogEquals(catalog2, catalogEntities.get(1)));
  }

  @Test
  public void testSchemaPutAndList() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test 1");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema1 =
        createSchema(
            1L,
            "test_schema1",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema 1");
    SchemaEntity schema2 =
        createSchema(
            2L,
            "test_schema2",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema 2");
    List<SchemaEntity> beforeSchemaList =
        entityStore.list(schema1.namespace(), SchemaEntity.class, Entity.EntityType.SCHEMA);
    assertNotNull(beforeSchemaList);
    assertEquals(0, beforeSchemaList.size());

    entityStore.put(schema1, false);
    entityStore.put(schema2, false);
    List<SchemaEntity> schemaEntities =
        entityStore.list(schema1.namespace(), SchemaEntity.class, Entity.EntityType.SCHEMA).stream()
            .sorted(Comparator.comparing(SchemaEntity::id))
            .collect(Collectors.toList());
    assertNotNull(schemaEntities);
    assertEquals(2, schemaEntities.size());
    assertTrue(checkSchemaEquals(schema1, schemaEntities.get(0)));
    assertTrue(checkSchemaEquals(schema2, schemaEntities.get(1)));
  }

  @Test
  public void testTablePutAndList() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test 1");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    TableEntity table1 =
        createTable(
            1L, "test_table1", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    TableEntity table2 =
        createTable(
            2L, "test_table2", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));

    List<TableEntity> beforeTableList =
        entityStore.list(table1.namespace(), TableEntity.class, Entity.EntityType.TABLE);
    assertNotNull(beforeTableList);
    assertEquals(0, beforeTableList.size());

    entityStore.put(table1, false);
    entityStore.put(table2, false);
    List<TableEntity> tableEntities =
        entityStore.list(table1.namespace(), TableEntity.class, Entity.EntityType.TABLE).stream()
            .sorted(Comparator.comparing(TableEntity::id))
            .collect(Collectors.toList());
    assertNotNull(tableEntities);
    assertEquals(2, tableEntities.size());
    assertTrue(checkTableEquals(table1, tableEntities.get(0)));
    assertTrue(checkTableEquals(table2, tableEntities.get(1)));
  }

  @Test
  public void testFilesetPutAndList() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test 1");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    FilesetEntity fileset1 =
        createFileset(
            1L,
            "test_filese1",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset 1",
            "hdfs://localhost/test1");
    FilesetEntity fileset2 =
        createFileset(
            2L,
            "test_filese2",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset 2",
            "hdfs://localhost/test2");

    List<FilesetEntity> beforeFilesetList =
        entityStore.list(fileset1.namespace(), FilesetEntity.class, Entity.EntityType.FILESET);
    assertNotNull(beforeFilesetList);
    assertEquals(0, beforeFilesetList.size());

    entityStore.put(fileset1, false);
    entityStore.put(fileset2, false);
    List<FilesetEntity> filesetEntities =
        entityStore.list(fileset1.namespace(), FilesetEntity.class, Entity.EntityType.FILESET)
            .stream()
            .sorted(Comparator.comparing(FilesetEntity::id))
            .collect(Collectors.toList());
    assertNotNull(filesetEntities);
    assertEquals(2, filesetEntities.size());
    assertTrue(checkFilesetEquals(fileset1, filesetEntities.get(0)));
    assertTrue(checkFilesetEquals(fileset2, filesetEntities.get(1)));
  }

  @Test
  public void testMetalakePutAndDelete() throws IOException, InterruptedException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    assertNotNull(
        entityStore.get(metalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class));
    entityStore.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            entityStore.get(
                metalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class));

    // sleep 1s to make delete_at seconds differently
    Thread.sleep(1000);

    // test cascade delete
    BaseMetalake metalake1 = createMetalake(2L, "test_metalake1", "this is test");
    entityStore.put(metalake1, false);

    CatalogEntity subCatalog =
        createCatalog(
            1L, "test_catalog", Namespace.ofCatalog(metalake1.name()), "test cascade deleted");
    entityStore.put(subCatalog, false);

    SchemaEntity subSchema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake1.name(), subCatalog.name()),
            "test cascade deleted");
    entityStore.put(subSchema, false);

    TableEntity subTable =
        createTable(
            1L,
            "test_table",
            Namespace.ofTable(metalake1.name(), subCatalog.name(), subSchema.name()));
    entityStore.put(subTable, false);

    FilesetEntity subFileset =
        createFileset(
            1L,
            "test_fileset",
            Namespace.ofFileset(metalake1.name(), subCatalog.name(), subSchema.name()),
            "test cascade deleted",
            "hdfs://localhost/test");
    entityStore.put(subFileset, false);

    // cascade is false
    assertThrows(
        NonEmptyEntityException.class,
        () -> entityStore.delete(metalake1.nameIdentifier(), Entity.EntityType.METALAKE, false));

    // cascade is true
    entityStore.delete(metalake1.nameIdentifier(), Entity.EntityType.METALAKE, true);
    assertFalse(entityStore.exists(metalake1.nameIdentifier(), Entity.EntityType.METALAKE));
    assertFalse(entityStore.exists(subCatalog.nameIdentifier(), Entity.EntityType.CATALOG));
    assertFalse(entityStore.exists(subSchema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertFalse(entityStore.exists(subTable.nameIdentifier(), Entity.EntityType.TABLE));
    assertFalse(entityStore.exists(subFileset.nameIdentifier(), Entity.EntityType.FILESET));
  }

  @Test
  public void testCatalogPutAndDelete() throws IOException, InterruptedException {
    BaseMetalake metalake = createMetalake(3L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(2L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    assertNotNull(
        entityStore.get(catalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class));
    entityStore.delete(catalog.nameIdentifier(), Entity.EntityType.CATALOG, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            entityStore.get(
                catalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class));

    // sleep 1s to make delete_at seconds differently
    Thread.sleep(1000);

    // test cascade delete
    CatalogEntity catalog1 =
        createCatalog(
            3L, "test_catalog1", Namespace.ofCatalog(metalake.name()), "test cascade deleted");
    entityStore.put(catalog1, false);

    SchemaEntity subSchema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog1.name()),
            "test cascade deleted");
    entityStore.put(subSchema, false);

    TableEntity subTable =
        createTable(
            1L,
            "test_table",
            Namespace.ofTable(metalake.name(), catalog1.name(), subSchema.name()));
    entityStore.put(subTable, false);

    FilesetEntity subFileset =
        createFileset(
            1L,
            "test_fileset",
            Namespace.ofFileset(metalake.name(), catalog1.name(), subSchema.name()),
            "test cascade deleted",
            "hdfs://localhost/test");
    entityStore.put(subFileset, false);

    // cascade is false
    assertThrows(
        NonEmptyEntityException.class,
        () -> entityStore.delete(catalog1.nameIdentifier(), Entity.EntityType.CATALOG, false));

    // cascade is true
    entityStore.delete(catalog1.nameIdentifier(), Entity.EntityType.CATALOG, true);
    assertFalse(entityStore.exists(catalog1.nameIdentifier(), Entity.EntityType.CATALOG));
    assertFalse(entityStore.exists(subSchema.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertFalse(entityStore.exists(subTable.nameIdentifier(), Entity.EntityType.TABLE));
    assertFalse(entityStore.exists(subFileset.nameIdentifier(), Entity.EntityType.FILESET));
  }

  @Test
  public void testSchemaPutAndDelete() throws IOException, InterruptedException {
    BaseMetalake metalake = createMetalake(3L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(2L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            2L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    assertNotNull(
        entityStore.get(schema.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));
    entityStore.delete(schema.nameIdentifier(), Entity.EntityType.SCHEMA, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            entityStore.get(schema.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class));

    // sleep 1s to make delete_at seconds differently
    Thread.sleep(1000);

    // test cascade delete
    SchemaEntity schema1 =
        createSchema(
            3L,
            "test_schema1",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "test cascade deleted");
    entityStore.put(schema1, false);

    TableEntity subTable =
        createTable(
            1L, "test_table", Namespace.ofTable(metalake.name(), catalog.name(), schema1.name()));
    entityStore.put(subTable, false);

    FilesetEntity subFileset =
        createFileset(
            1L,
            "test_fileset",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema1.name()),
            "test cascade deleted",
            "hdfs://localhost/test");
    entityStore.put(subFileset, false);

    // cascade is false
    assertThrows(
        NonEmptyEntityException.class,
        () -> entityStore.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, false));

    // cascade is true
    entityStore.delete(schema1.nameIdentifier(), Entity.EntityType.SCHEMA, true);
    assertFalse(entityStore.exists(schema1.nameIdentifier(), Entity.EntityType.SCHEMA));
    assertFalse(entityStore.exists(subTable.nameIdentifier(), Entity.EntityType.TABLE));
    assertFalse(entityStore.exists(subFileset.nameIdentifier(), Entity.EntityType.FILESET));
  }

  @Test
  public void testTablePutAndDelete() throws IOException {
    BaseMetalake metalake = createMetalake(3L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(2L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            2L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    TableEntity table =
        createTable(
            2L, "test_table", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    entityStore.put(table, false);

    assertNotNull(
        entityStore.get(table.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
    entityStore.delete(table.nameIdentifier(), Entity.EntityType.TABLE, false);

    assertThrows(
        NoSuchEntityException.class,
        () -> entityStore.get(table.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class));
  }

  @Test
  public void testFilesetPutAndDelete() throws IOException, InterruptedException {
    BaseMetalake metalake = createMetalake(3L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(2L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            2L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    FilesetEntity fileset =
        createFileset(
            2L,
            "test_fileset",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset test",
            "hdfs://localhost/test");
    entityStore.put(fileset, false);

    assertNotNull(
        entityStore.get(fileset.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
    entityStore.delete(fileset.nameIdentifier(), Entity.EntityType.FILESET, false);

    assertThrows(
        NoSuchEntityException.class,
        () ->
            entityStore.get(
                fileset.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class));
  }

  @Test
  public void testMetalakePutAndUpdate() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    assertThrows(
        RuntimeException.class,
        () ->
            entityStore.update(
                metalake.nameIdentifier(),
                BaseMetalake.class,
                Entity.EntityType.METALAKE,
                m -> {
                  BaseMetalake.Builder builder =
                      new BaseMetalake.Builder()
                          // Change the id, which is not allowed
                          .withId(2L)
                          .withName("test_metalake2")
                          .withComment("this is test 2")
                          .withProperties(new HashMap<>())
                          .withAuditInfo((AuditInfo) m.auditInfo())
                          .withVersion(m.getVersion());
                  return builder.build();
                }));

    AuditInfo changedAuditInfo =
        AuditInfo.builder().withCreator("changed_creator").withCreateTime(Instant.now()).build();
    BaseMetalake updatedMetalake =
        entityStore.update(
            metalake.nameIdentifier(),
            BaseMetalake.class,
            Entity.EntityType.METALAKE,
            m -> {
              BaseMetalake.Builder builder =
                  new BaseMetalake.Builder()
                      .withId(m.id())
                      .withName("test_metalake2")
                      .withComment("this is test 2")
                      .withProperties(new HashMap<>())
                      .withAuditInfo(changedAuditInfo)
                      .withVersion(m.getVersion());
              return builder.build();
            });

    BaseMetalake storedMetalake =
        entityStore.get(
            updatedMetalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class);
    assertEquals(metalake.id(), storedMetalake.id());
    assertEquals("test_metalake2", updatedMetalake.name());
    assertEquals("this is test 2", updatedMetalake.comment());
    assertEquals(changedAuditInfo.creator(), updatedMetalake.auditInfo().creator());

    BaseMetalake metalake3 = createMetalake(3L, "test_metalake3", "this is test 3");
    entityStore.put(metalake3, false);

    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            entityStore.update(
                metalake3.nameIdentifier(),
                BaseMetalake.class,
                Entity.EntityType.METALAKE,
                m -> {
                  BaseMetalake.Builder builder =
                      new BaseMetalake.Builder()
                          .withId(metalake3.id())
                          // metalake name already exists
                          .withName("test_metalake2")
                          .withComment(metalake3.comment())
                          .withProperties(new HashMap<>())
                          .withAuditInfo((AuditInfo) m.auditInfo())
                          .withVersion(m.getVersion());
                  return builder.build();
                }));
  }

  @Test
  public void testCatalogPutAndUpdate() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is catalog test");
    entityStore.put(catalog, false);

    assertThrows(
        RuntimeException.class,
        () ->
            entityStore.update(
                catalog.nameIdentifier(),
                CatalogEntity.class,
                Entity.EntityType.CATALOG,
                c -> {
                  CatalogEntity.Builder builder =
                      CatalogEntity.builder()
                          // Change the id, which is not allowed
                          .withId(2L)
                          .withName("test_catalog2")
                          .withNamespace(Namespace.ofCatalog(metalake.name()))
                          .withType(Catalog.Type.RELATIONAL)
                          .withProvider("test")
                          .withComment("this is catalog test 2")
                          .withProperties(new HashMap<>())
                          .withAuditInfo((AuditInfo) c.auditInfo());
                  return builder.build();
                }));

    AuditInfo changedAuditInfo =
        AuditInfo.builder().withCreator("changed_creator").withCreateTime(Instant.now()).build();
    CatalogEntity updatedCatalog =
        entityStore.update(
            catalog.nameIdentifier(),
            CatalogEntity.class,
            Entity.EntityType.CATALOG,
            c -> {
              CatalogEntity.Builder builder =
                  CatalogEntity.builder()
                      .withId(c.id())
                      .withName("test_catalog2")
                      .withNamespace(Namespace.ofCatalog(metalake.name()))
                      .withType(Catalog.Type.RELATIONAL)
                      .withProvider("test")
                      .withComment("this is catalog test 2")
                      .withProperties(new HashMap<>())
                      .withAuditInfo(changedAuditInfo);
              return builder.build();
            });

    CatalogEntity storedCatalog =
        entityStore.get(
            updatedCatalog.nameIdentifier(), Entity.EntityType.CATALOG, CatalogEntity.class);

    assertEquals(catalog.id(), storedCatalog.id());
    assertEquals("test_catalog2", updatedCatalog.name());
    assertEquals("this is catalog test 2", updatedCatalog.getComment());
    assertEquals(changedAuditInfo.creator(), updatedCatalog.auditInfo().creator());

    CatalogEntity catalog3 =
        createCatalog(
            3L, "test_catalog3", Namespace.ofCatalog(metalake.name()), "this is catalog test 3");
    entityStore.put(catalog3, false);
    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            entityStore.update(
                catalog3.nameIdentifier(),
                CatalogEntity.class,
                Entity.EntityType.CATALOG,
                c -> {
                  CatalogEntity.Builder builder =
                      CatalogEntity.builder()
                          .withId(catalog3.id())
                          // catalog name already exists
                          .withName("test_catalog2")
                          .withNamespace(Namespace.ofCatalog(metalake.name()))
                          .withType(Catalog.Type.RELATIONAL)
                          .withProvider("test")
                          .withComment(catalog3.getComment())
                          .withProperties(new HashMap<>())
                          .withAuditInfo((AuditInfo) c.auditInfo());
                  return builder.build();
                }));
  }

  @Test
  public void testSchemaPutAndUpdate() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is catalog test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test");
    entityStore.put(schema, false);

    assertThrows(
        RuntimeException.class,
        () ->
            entityStore.update(
                schema.nameIdentifier(),
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                s -> {
                  SchemaEntity.Builder builder =
                      new SchemaEntity.Builder()
                          // Change the id, which is not allowed
                          .withId(2L)
                          .withName("test_schema2")
                          .withNamespace(Namespace.ofSchema(metalake.name(), catalog.name()))
                          .withComment("this is schema test 2")
                          .withProperties(new HashMap<>())
                          .withAuditInfo(s.auditInfo());
                  return builder.build();
                }));

    AuditInfo changedAuditInfo =
        AuditInfo.builder().withCreator("changed_creator").withCreateTime(Instant.now()).build();
    SchemaEntity updatedSchema =
        entityStore.update(
            schema.nameIdentifier(),
            SchemaEntity.class,
            Entity.EntityType.SCHEMA,
            s -> {
              SchemaEntity.Builder builder =
                  new SchemaEntity.Builder()
                      .withId(s.id())
                      .withName("test_schema2")
                      .withNamespace(Namespace.ofSchema(metalake.name(), catalog.name()))
                      .withComment("this is schema test 2")
                      .withProperties(new HashMap<>())
                      .withAuditInfo(changedAuditInfo);
              return builder.build();
            });

    SchemaEntity storedSchema =
        entityStore.get(
            updatedSchema.nameIdentifier(), Entity.EntityType.SCHEMA, SchemaEntity.class);

    assertEquals(catalog.id(), storedSchema.id());
    assertEquals("test_schema2", storedSchema.name());
    assertEquals("this is schema test 2", storedSchema.comment());
    assertEquals(changedAuditInfo.creator(), storedSchema.auditInfo().creator());

    SchemaEntity schema3 =
        createSchema(
            3L,
            "test_schema3",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test 3");
    entityStore.put(schema3, false);

    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            entityStore.update(
                schema3.nameIdentifier(),
                SchemaEntity.class,
                Entity.EntityType.SCHEMA,
                s -> {
                  SchemaEntity.Builder builder =
                      new SchemaEntity.Builder()
                          .withId(schema3.id())
                          // schema name already exists
                          .withName("test_schema2")
                          .withNamespace(Namespace.ofSchema(metalake.name(), catalog.name()))
                          .withComment(s.comment())
                          .withProperties(new HashMap<>())
                          .withAuditInfo(s.auditInfo());
                  return builder.build();
                }));
  }

  @Test
  public void testTablePutAndUpdate() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is catalog test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test");
    entityStore.put(schema, false);

    TableEntity table =
        createTable(
            1L, "test_table", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    entityStore.put(table, false);

    assertThrows(
        RuntimeException.class,
        () ->
            entityStore.update(
                table.nameIdentifier(),
                TableEntity.class,
                Entity.EntityType.TABLE,
                s -> {
                  TableEntity.Builder builder =
                      new TableEntity.Builder()
                          // Change the id, which is not allowed
                          .withId(2L)
                          .withName("test_table2")
                          .withNamespace(
                              Namespace.ofTable(metalake.name(), catalog.name(), schema.name()))
                          .withAuditInfo(s.auditInfo());
                  return builder.build();
                }));

    AuditInfo changedAuditInfo =
        AuditInfo.builder().withCreator("changed_creator").withCreateTime(Instant.now()).build();
    TableEntity updatedTable =
        entityStore.update(
            table.nameIdentifier(),
            TableEntity.class,
            Entity.EntityType.TABLE,
            s -> {
              TableEntity.Builder builder =
                  new TableEntity.Builder()
                      .withId(s.id())
                      .withName("test_table2")
                      .withNamespace(
                          Namespace.ofTable(metalake.name(), catalog.name(), schema.name()))
                      .withAuditInfo(changedAuditInfo);
              return builder.build();
            });

    TableEntity storedTable =
        entityStore.get(updatedTable.nameIdentifier(), Entity.EntityType.TABLE, TableEntity.class);

    assertEquals(table.id(), storedTable.id());
    assertEquals("test_table2", storedTable.name());
    assertEquals(changedAuditInfo.creator(), storedTable.auditInfo().creator());

    TableEntity table3 =
        createTable(
            3L, "test_table3", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    entityStore.put(table3, false);

    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            entityStore.update(
                table3.nameIdentifier(),
                TableEntity.class,
                Entity.EntityType.TABLE,
                s -> {
                  TableEntity.Builder builder =
                      new TableEntity.Builder()
                          .withId(table3.id())
                          // table name already exists
                          .withName("test_table2")
                          .withNamespace(
                              Namespace.ofTable(metalake.name(), catalog.name(), schema.name()))
                          .withAuditInfo(s.auditInfo());
                  return builder.build();
                }));
  }

  @Test
  public void testFilesetPutAndUpdate() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(
            1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is catalog test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L,
            "test_schema",
            Namespace.ofSchema(metalake.name(), catalog.name()),
            "this is schema test");
    entityStore.put(schema, false);

    FilesetEntity fileset =
        createFileset(
            1L,
            "test_fileset",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset test",
            "hdfs://localhost/test");
    entityStore.put(fileset, false);

    assertThrows(
        RuntimeException.class,
        () ->
            entityStore.update(
                fileset.nameIdentifier(),
                FilesetEntity.class,
                Entity.EntityType.FILESET,
                f -> {
                  FilesetEntity.Builder builder =
                      new FilesetEntity.Builder()
                          // Change the id, which is not allowed
                          .withId(2L)
                          .withName("test_fileset2")
                          .withNamespace(
                              Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()))
                          .withComment(f.comment())
                          .withProperties(f.properties())
                          .withStorageLocation(f.storageLocation())
                          .withFilesetType(f.filesetType())
                          .withAuditInfo(f.auditInfo());
                  return builder.build();
                }));

    AuditInfo changedAuditInfo =
        AuditInfo.builder().withCreator("changed_creator").withCreateTime(Instant.now()).build();
    // update version fields and normal fields
    FilesetEntity updatedFileset =
        entityStore.update(
            fileset.nameIdentifier(),
            FilesetEntity.class,
            Entity.EntityType.FILESET,
            f -> {
              FilesetEntity.Builder builder =
                  new FilesetEntity.Builder()
                      .withId(f.id())
                      .withName("test_fileset2")
                      .withNamespace(
                          Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()))
                      .withFilesetType(f.filesetType())
                      .withComment("this is fileset test 2")
                      .withStorageLocation("hdfs://localhost/test2")
                      .withProperties(f.properties())
                      .withAuditInfo(changedAuditInfo);
              return builder.build();
            });

    FilesetEntity storedFileset =
        entityStore.get(
            updatedFileset.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class);

    assertEquals(fileset.id(), storedFileset.id());
    assertEquals("test_fileset2", storedFileset.name());
    assertEquals(changedAuditInfo.creator(), storedFileset.auditInfo().creator());
    assertEquals("this is fileset test 2", storedFileset.comment());
    assertEquals("hdfs://localhost/test2", storedFileset.storageLocation());

    FilesetEntity fileset3 =
        createFileset(
            3L,
            "test_fileset3",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is fileset test 3",
            "hdfs://localhost/test3");
    entityStore.put(fileset3, false);

    assertThrows(
        EntityAlreadyExistsException.class,
        () ->
            entityStore.update(
                fileset3.nameIdentifier(),
                FilesetEntity.class,
                Entity.EntityType.FILESET,
                f -> {
                  FilesetEntity.Builder builder =
                      new FilesetEntity.Builder()
                          .withId(fileset3.id())
                          // fileset name already exists
                          .withName("test_fileset2")
                          .withNamespace(
                              Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()))
                          .withFilesetType(f.filesetType())
                          .withStorageLocation(f.storageLocation())
                          .withComment(f.comment())
                          .withProperties(f.properties())
                          .withAuditInfo(f.auditInfo());
                  return builder.build();
                }));

    // only update normal fields
    FilesetEntity updatedFileset2 =
        entityStore.update(
            fileset3.nameIdentifier(),
            FilesetEntity.class,
            Entity.EntityType.FILESET,
            f -> {
              FilesetEntity.Builder builder =
                  new FilesetEntity.Builder()
                      .withId(f.id())
                      .withName("test_fileset4")
                      .withNamespace(
                          Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()))
                      .withFilesetType(f.filesetType())
                      .withComment(f.comment())
                      .withStorageLocation(f.storageLocation())
                      .withProperties(f.properties())
                      .withAuditInfo(changedAuditInfo);
              return builder.build();
            });

    FilesetEntity storedFileset2 =
        entityStore.get(
            updatedFileset2.nameIdentifier(), Entity.EntityType.FILESET, FilesetEntity.class);

    assertEquals(fileset3.id(), storedFileset2.id());
    assertEquals("test_fileset4", storedFileset2.name());
    assertEquals(changedAuditInfo.creator(), storedFileset2.auditInfo().creator());
    assertEquals(fileset3.comment(), storedFileset2.comment());
    assertEquals(fileset3.storageLocation(), storedFileset2.storageLocation());
  }

  @Test
  public void testMetalakePutAndExists() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);
    assertTrue(entityStore.exists(metalake.nameIdentifier(), Entity.EntityType.METALAKE));
  }

  @Test
  public void testCatalogPutAndExists() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    assertTrue(entityStore.exists(catalog.nameIdentifier(), Entity.EntityType.CATALOG));
  }

  @Test
  public void testSchemaPutAndExists() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    assertTrue(entityStore.exists(schema.nameIdentifier(), Entity.EntityType.SCHEMA));
  }

  @Test
  public void testTablePutAndExists() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    TableEntity table =
        createTable(
            1L, "test_table", Namespace.ofTable(metalake.name(), catalog.name(), schema.name()));
    entityStore.put(table, false);

    assertTrue(entityStore.exists(table.nameIdentifier(), Entity.EntityType.TABLE));
  }

  @Test
  public void testFilesetPutAndExists() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);

    CatalogEntity catalog =
        createCatalog(1L, "test_catalog", Namespace.ofCatalog(metalake.name()), "this is test");
    entityStore.put(catalog, false);

    SchemaEntity schema =
        createSchema(
            1L, "test_schema", Namespace.ofSchema(metalake.name(), catalog.name()), "this is test");
    entityStore.put(schema, false);

    FilesetEntity fileset =
        createFileset(
            1L,
            "test_table",
            Namespace.ofFileset(metalake.name(), catalog.name(), schema.name()),
            "this is test",
            "hdfs://localhost/test");
    entityStore.put(fileset, false);

    assertTrue(entityStore.exists(fileset.nameIdentifier(), Entity.EntityType.FILESET));
  }

  private static BaseMetalake createMetalake(Long id, String name, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    return new BaseMetalake.Builder()
        .withId(id)
        .withName(name)
        .withComment(comment)
        .withProperties(new HashMap<>())
        .withAuditInfo(auditInfo)
        .withVersion(SchemaVersion.V_0_1)
        .build();
  }

  private static boolean checkMetalakeEquals(BaseMetalake expected, BaseMetalake actual) {
    return expected.id().equals(actual.id())
        && expected.name().equals(actual.name())
        && expected.comment().equals(actual.comment())
        && expected.properties().equals(actual.properties())
        && expected.auditInfo().equals(actual.auditInfo())
        && expected.getVersion().equals(actual.getVersion());
  }

  private static CatalogEntity createCatalog(
      Long id, String name, Namespace namespace, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    return CatalogEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withType(Catalog.Type.RELATIONAL)
        .withProvider("test")
        .withComment(comment)
        .withProperties(new HashMap<>())
        .withAuditInfo(auditInfo)
        .build();
  }

  private static boolean checkCatalogEquals(CatalogEntity expected, CatalogEntity actual) {
    return expected.id().equals(actual.id())
        && expected.name().equals(actual.name())
        && expected.namespace().equals(actual.namespace())
        && expected.getComment().equals(actual.getComment())
        && expected.getType().equals(actual.getType())
        && expected.getProvider().equals(actual.getProvider())
        && expected.getProperties() != null
        && expected.getProperties().equals(actual.getProperties())
        && expected.auditInfo().equals(actual.auditInfo());
  }

  private static SchemaEntity createSchema(
      Long id, String name, Namespace namespace, String comment) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    return new SchemaEntity.Builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(comment)
        .withProperties(new HashMap<>())
        .withAuditInfo(auditInfo)
        .build();
  }

  private static boolean checkSchemaEquals(SchemaEntity expected, SchemaEntity actual) {
    return expected.id().equals(actual.id())
        && expected.name().equals(actual.name())
        && expected.namespace().equals(actual.namespace())
        && expected.comment().equals(actual.comment())
        && expected.properties() != null
        && expected.properties().equals(actual.properties())
        && expected.auditInfo().equals(actual.auditInfo());
  }

  private static TableEntity createTable(Long id, String name, Namespace namespace) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    return new TableEntity.Builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withAuditInfo(auditInfo)
        .build();
  }

  private static boolean checkTableEquals(TableEntity expected, TableEntity actual) {
    return expected.id().equals(actual.id())
        && expected.name().equals(actual.name())
        && expected.namespace().equals(actual.namespace())
        && expected.auditInfo().equals(actual.auditInfo());
  }

  private static FilesetEntity createFileset(
      Long id, String name, Namespace namespace, String comment, String location) {
    AuditInfo auditInfo =
        AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build();
    return new FilesetEntity.Builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withFilesetType(Fileset.Type.MANAGED)
        .withComment(comment)
        .withAuditInfo(auditInfo)
        .withStorageLocation(location)
        .withProperties(new HashMap<>())
        .build();
  }

  private static boolean checkFilesetEquals(FilesetEntity expected, FilesetEntity actual) {
    return expected.id().equals(actual.id())
        && expected.name().equals(actual.name())
        && expected.filesetType() == actual.filesetType()
        && expected.comment().equals(actual.comment())
        && expected.storageLocation().equals(actual.storageLocation())
        && expected.properties() != null
        && expected.properties().equals(actual.properties())
        && expected.namespace().equals(actual.namespace())
        && expected.auditInfo().equals(actual.auditInfo());
  }

  private static void truncateAllTables() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          String query = "SHOW TABLES";
          List<String> tableList = new ArrayList<>();
          try (ResultSet rs = statement.executeQuery(query)) {
            while (rs.next()) {
              tableList.add(rs.getString(1));
            }
          }
          for (String table : tableList) {
            statement.execute("TRUNCATE TABLE " + table);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Clear table failed", e);
    }
  }

  private static void dropAllTables() {
    try (SqlSession sqlSession =
        SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true)) {
      try (Connection connection = sqlSession.getConnection()) {
        try (Statement statement = connection.createStatement()) {
          String query = "SHOW TABLES";
          List<String> tableList = new ArrayList<>();
          try (ResultSet rs = statement.executeQuery(query)) {
            while (rs.next()) {
              tableList.add(rs.getString(1));
            }
          }
          for (String table : tableList) {
            statement.execute("DROP TABLE " + table);
          }
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Drop table failed", e);
    }
  }
}
