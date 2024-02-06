/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.DEFAULT_ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_MYSQL_BACKEND_DRIVER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_MYSQL_BACKEND_URL;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_MYSQL_BACKEND_USER;
import static com.datastrato.gravitino.Configs.ENTITY_RELATIONAL_STORE;
import static com.datastrato.gravitino.Configs.ENTITY_STORE;
import static com.datastrato.gravitino.Configs.RELATIONAL_ENTITY_STORE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityStore;
import com.datastrato.gravitino.EntityStoreFactory;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.SchemaVersion;
import com.datastrato.gravitino.storage.relational.mysql.session.SqlSessionFactoryHelper;
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
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRelationalEntityStore {
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
    Mockito.when(config.get(ENTITY_RELATIONAL_MYSQL_BACKEND_URL))
        .thenReturn(String.format("jdbc:h2:%s;DB_CLOSE_DELAY=-1;MODE=MYSQL", DB_DIR));
    Mockito.when(config.get(ENTITY_RELATIONAL_MYSQL_BACKEND_USER)).thenReturn("sa");
    Mockito.when(config.get(ENTITY_RELATIONAL_MYSQL_BACKEND_DRIVER)).thenReturn("org.h2.Driver");
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
  public static void tearDown() throws IOException {
    dropAllTables();
    entityStore.close();
    File dir = new File(DB_DIR);
    if (dir.exists()) {
      dir.delete();
    }
  }

  @Test
  public void testPutAndGet() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);
    BaseMetalake insertedMetalake =
        entityStore.get(metalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class);
    assertNotNull(insertedMetalake);
    assertTrue(checkMetalakeEquals(metalake, insertedMetalake));

    // overwrite false
    BaseMetalake duplicateMetalake = createMetalake(1L, "test_metalake", "this is test");
    assertThrows(RuntimeException.class, () -> entityStore.put(duplicateMetalake, false));

    // overwrite true
    BaseMetalake overittenMetalake = createMetalake(1L, "test_metalake2", "this is test2");
    entityStore.put(overittenMetalake, true);
    BaseMetalake insertedMetalake1 =
        entityStore.get(
            overittenMetalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class);
    assertEquals(
        1,
        entityStore.list(Namespace.empty(), BaseMetalake.class, Entity.EntityType.METALAKE).size());
    assertEquals("test_metalake2", insertedMetalake1.name());
    assertEquals("this is test2", insertedMetalake1.comment());
  }

  @Test
  public void testPutAndList() throws IOException {
    BaseMetalake metalake1 = createMetalake(1L, "test_metalake1", "this is test 1");
    BaseMetalake metalake2 = createMetalake(2L, "test_metalake2", "this is test 2");
    List<BaseMetalake> beforePutList =
        entityStore.list(metalake1.namespace(), BaseMetalake.class, Entity.EntityType.METALAKE);
    assertNotNull(beforePutList);
    assertEquals(0, beforePutList.size());

    entityStore.put(metalake1, false);
    entityStore.put(metalake2, false);
    List<BaseMetalake> metalakes =
        entityStore.list(metalake1.namespace(), BaseMetalake.class, Entity.EntityType.METALAKE);
    assertNotNull(metalakes);
    assertEquals(2, metalakes.size());
    assertTrue(checkMetalakeEquals(metalake1, metalakes.get(0)));
    assertTrue(checkMetalakeEquals(metalake2, metalakes.get(1)));
  }

  @Test
  public void testPutAndDelete() throws IOException {
    BaseMetalake metalake = createMetalake(1L, "test_metalake", "this is test");
    entityStore.put(metalake, false);
    entityStore.delete(metalake.nameIdentifier(), Entity.EntityType.METALAKE, false);
    assertThrows(
        NoSuchEntityException.class,
        () ->
            entityStore.get(
                metalake.nameIdentifier(), Entity.EntityType.METALAKE, BaseMetalake.class));
  }

  @Test
  public void testPutAndUpdate() throws IOException {
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
