/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.operation;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.datastrato.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import com.datastrato.gravitino.catalog.jdbc.utils.DataSourceUtils;
import com.datastrato.gravitino.exceptions.SchemaAlreadyExistsException;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJdbcDatabaseOperations {

  private static DataSource DATA_SOURCE;
  private static JdbcExceptionConverter EXCEPTION_MAPPER;

  private static SqliteDatabaseOperations JDBC_DATABASE_OPERATIONS;

  private static File BASE_FILE_DIR;

  private static String FILE_PATH;
  private static String JDBC_URL;

  @BeforeAll
  public static void startup() throws IOException {
    BASE_FILE_DIR = Files.createTempDirectory("gravitino-jdbc").toFile();
    FILE_PATH = BASE_FILE_DIR.getPath() + "/test";
    JDBC_URL = "jdbc:sqlite:" + FILE_PATH;
    FileUtils.createParentDirectories(new File(FILE_PATH));
    createDataSource();
    createExceptionMapper();
    createJdbcDatabaseOperations();
  }

  @AfterAll
  public static void stop() {
    FileUtils.deleteQuietly(BASE_FILE_DIR);
  }

  private static void createExceptionMapper() {
    EXCEPTION_MAPPER = new SqliteExceptionConverter();
  }

  private static void createDataSource() {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), JDBC_URL);
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");
    DATA_SOURCE = DataSourceUtils.createDataSource(properties);
  }

  private static void createJdbcDatabaseOperations() {
    JDBC_DATABASE_OPERATIONS = new SqliteDatabaseOperations(BASE_FILE_DIR.getPath());
    JDBC_DATABASE_OPERATIONS.initialize(DATA_SOURCE, EXCEPTION_MAPPER, Collections.emptyMap());
  }

  @Test
  public void testOperationDatabase() {
    String database1 = "test";
    String database2 = "test2";
    // creat database
    Assertions.assertDoesNotThrow(() -> JDBC_DATABASE_OPERATIONS.create(database1, null, null));
    SchemaAlreadyExistsException illegalArgumentException =
        Assertions.assertThrows(
            SchemaAlreadyExistsException.class,
            () -> JDBC_DATABASE_OPERATIONS.create(database1, null, null));
    Assertions.assertTrue(
        StringUtils.contains(illegalArgumentException.getMessage(), "already exists"));
    Assertions.assertDoesNotThrow(() -> JDBC_DATABASE_OPERATIONS.create(database2, null, null));

    // list database
    List<String> listDatabases = JDBC_DATABASE_OPERATIONS.listDatabases();
    Assertions.assertEquals(2, listDatabases.size());
    Assertions.assertTrue(listDatabases.contains(database1));
    Assertions.assertTrue(listDatabases.contains(database2));

    // drop database
    JDBC_DATABASE_OPERATIONS.delete(database1);
    List<String> databases = JDBC_DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databases.contains(database1));
    Assertions.assertNotNull(JDBC_DATABASE_OPERATIONS.load(database2));
    JDBC_DATABASE_OPERATIONS.delete(database2);
    Assertions.assertNull(JDBC_DATABASE_OPERATIONS.load(database2));
  }
}
