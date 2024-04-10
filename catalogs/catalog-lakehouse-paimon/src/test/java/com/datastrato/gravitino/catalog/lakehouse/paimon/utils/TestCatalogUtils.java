/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon.utils;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.METASTORE;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.WAREHOUSE;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.utils.CatalogUtils.loadCatalogBackend;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTORECONNECTURLKEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.factories.FactoryException;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.HadoopUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests for {@link CatalogUtils}. */
public class TestCatalogUtils {

  @Test
  void testLoadCatalogBackend() throws Exception {
    // Test load FileSystemCatalog for filesystem metastore.
    assertCatalog(PaimonCatalogBackend.FILESYSTEM.name(), FileSystemCatalog.class);
    // Test load HiveCatalog for hive metastore.
    try (MockedStatic<HadoopUtils> mocked = Mockito.mockStatic(HadoopUtils.class)) {
      HiveConf hiveConf = new HiveConf();
      hiveConf.setVar(METASTORECONNECTURLKEY, connectionUrl());
      mocked
          .when(() -> HadoopUtils.getHadoopConfiguration(Mockito.any(Options.class)))
          .thenReturn(hiveConf);
      assertCatalog(PaimonCatalogBackend.HIVE.name(), HiveCatalog.class);
    }
    // Test load catalog exception for other metastore.
    assertThrowsExactly(FactoryException.class, () -> assertCatalog("other", catalog -> {}));
  }

  private void assertCatalog(String metastore, Class<?> expected) throws Exception {
    assertCatalog(metastore, catalog -> assertEquals(expected, catalog.getClass()));
    assertCatalog(
        metastore.toLowerCase(Locale.ROOT), catalog -> assertEquals(expected, catalog.getClass()));
  }

  private void assertCatalog(String metastore, Consumer<Catalog> consumer) throws Exception {
    try (Catalog catalog =
        loadCatalogBackend(
            new PaimonConfig(
                ImmutableMap.of(
                    METASTORE.getKey(), metastore, WAREHOUSE.getKey(), "/warehouse")))) {
      consumer.accept(catalog);
    }
  }

  private String connectionUrl() throws SQLException, IOException {
    String connectionUrl = String.format("jdbc:derby:memory:%s;create=true", UUID.randomUUID());
    setupMetastore(connectionUrl);
    return connectionUrl;
  }

  private void setupMetastore(String connectionUrl) throws SQLException, IOException {
    InputStream inputStream =
        ClassLoader.getSystemClassLoader().getResourceAsStream("hive-schema-3.1.0.derby.sql");
    if (inputStream != null) {
      try (Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        runScript(DriverManager.getConnection(connectionUrl), reader);
      }
    }
  }

  private void runScript(Connection conn, Reader reader) throws IOException, SQLException {
    Statement statement = null;
    try {
      LineNumberReader lineNumberReader = new LineNumberReader(reader);
      String line;
      StringBuilder command = null;
      while ((line = lineNumberReader.readLine()) != null) {
        if (command == null) {
          command = new StringBuilder();
        }
        String script = line.trim();
        if (!script.isEmpty()) {
          if (script.endsWith(";")) {
            command.append(line, 0, line.lastIndexOf(";"));
            command.append(" ");
            statement = conn.createStatement();
            statement.execute(command.toString());
            command = null;
          } else if (!script.startsWith("--") && !script.startsWith("//")) {
            command.append(line);
            command.append(" ");
          }
        }
      }
    } finally {
      if (statement != null) {
        statement.close();
      }
    }
  }
}
