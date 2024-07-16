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
package org.apache.gravitino.catalog.jdbc.operation;

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
import org.apache.gravitino.catalog.jdbc.config.JdbcConfig;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.catalog.jdbc.converter.SqliteExceptionConverter;
import org.apache.gravitino.catalog.jdbc.utils.DataSourceUtils;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
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
    Assertions.assertTrue(JDBC_DATABASE_OPERATIONS.delete(database1), "database should be dropped");
    List<String> databases = JDBC_DATABASE_OPERATIONS.listDatabases();
    Assertions.assertFalse(databases.contains(database1));
    Assertions.assertNotNull(JDBC_DATABASE_OPERATIONS.load(database2));
    Assertions.assertTrue(JDBC_DATABASE_OPERATIONS.delete(database2), "database should be dropped");
    Assertions.assertNull(JDBC_DATABASE_OPERATIONS.load(database2));

    // drop non-existent database
    Assertions.assertFalse(
        JDBC_DATABASE_OPERATIONS.delete(database1), "database should be non-existent");
  }
}
