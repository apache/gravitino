/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_URL_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql.MySQLConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql.PostgreSQLConnectorAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.datastrato.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJDBCCatalogPropertyConverter {

  @Test
  public void testTrinoPropertyKeyToGravitino() {
    PropertyConverter propertyConverter = new JDBCCatalogPropertyConverter();
    Map<String, String> gravitinoProperties =
        ImmutableMap.of(
            "jdbc-url", "jdbc:mysql://localhost:3306",
            "jdbc-user", "root",
            "jdbc-password", "root");

    Map<String, String> trinoProperties =
        propertyConverter.gravitinoToEngineProperties(gravitinoProperties);
    Assertions.assertEquals(
        trinoProperties.get(JDBC_CONNECTION_URL_KEY), "jdbc:mysql://localhost:3306");
    Assertions.assertEquals(trinoProperties.get(JDBC_CONNECTION_USER_KEY), "root");
    Assertions.assertEquals(trinoProperties.get(JDBC_CONNECTION_PASSWORD_KEY), "root");

    Map<String, String> gravitinoPropertiesWithoutPassword =
        ImmutableMap.of(
            "jdbc-url", "jdbc:mysql://localhost:3306",
            "jdbc-user", "root");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          propertyConverter.gravitinoToEngineProperties(gravitinoPropertiesWithoutPassword);
        });
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildPostgreSqlConnectorProperties() throws Exception {
    String name = "test_catalog";
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("jdbc-url", "jdbc:postgresql://localhost:5432/test")
            .put("jdbc-user", "test")
            .put("jdbc-password", "test")
            .put("trino.bypass.join-pushdown.strategy", "EAGER")
            .put("unknown-key", "1")
            .put("trino.bypass.unknown-key", "1")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "jdbc-postgresql", "test catalog", Catalog.Type.RELATIONAL, properties);
    PostgreSQLConnectorAdapter adapter = new PostgreSQLConnectorAdapter();

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test converted properties
    Assertions.assertEquals(config.get("connection-url"), "jdbc:postgresql://localhost:5432/test");
    Assertions.assertEquals(config.get("connection-user"), "test");
    Assertions.assertEquals(config.get("connection-password"), "test");

    // test trino passing properties
    Assertions.assertEquals(config.get("join-pushdown.strategy"), "EAGER");

    // test unknown properties
    Assertions.assertNull(config.get("hive.unknown-key"));
    Assertions.assertNull(config.get("trino.bypass.unknown-key"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBuildMySqlConnectorProperties() throws Exception {
    String name = "test_catalog";
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put("jdbc-url", "jdbc:mysql://localhost:5432/test")
            .put("jdbc-user", "test")
            .put("jdbc-password", "test")
            .put("trino.bypass.join-pushdown.strategy", "EAGER")
            .put("unknown-key", "1")
            .put("trino.bypass.unknown-key", "1")
            .build();
    Catalog mockCatalog =
        TestGravitinoCatalog.mockCatalog(
            name, "jdbc-postgresql", "test catalog", Catalog.Type.RELATIONAL, properties);
    MySQLConnectorAdapter adapter = new MySQLConnectorAdapter();

    Map<String, String> config =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test converted properties
    Assertions.assertEquals(config.get("connection-url"), "jdbc:mysql://localhost:5432/test");
    Assertions.assertEquals(config.get("connection-user"), "test");
    Assertions.assertEquals(config.get("connection-password"), "test");

    // test trino passing properties
    Assertions.assertEquals(config.get("join-pushdown.strategy"), "EAGER");

    // test unknown properties
    Assertions.assertNull(config.get("hive.unknown-key"));
    Assertions.assertNull(config.get("trino.bypass.unknown-key"));
  }
}
