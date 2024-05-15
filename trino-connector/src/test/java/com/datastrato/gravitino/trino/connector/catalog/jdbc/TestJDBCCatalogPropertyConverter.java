/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_URL_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY;

import com.datastrato.gravitino.client.api.Catalog;
import com.datastrato.gravitino.catalog.property.PropertyConverter;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.mysql.MySQLConnectorAdapter;
import com.datastrato.gravitino.trino.connector.catalog.jdbc.postgresql.PostgreSQLConnectorAdapter;
import com.datastrato.gravitino.trino.connector.metadata.GravitinoCatalog;
import com.datastrato.gravitino.trino.connector.metadata.TestGravitinoCatalog;
import java.util.Map;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

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
    Assert.assertEquals(
        trinoProperties.get(JDBC_CONNECTION_URL_KEY), "jdbc:mysql://localhost:3306");
    Assert.assertEquals(trinoProperties.get(JDBC_CONNECTION_USER_KEY), "root");
    Assert.assertEquals(trinoProperties.get(JDBC_CONNECTION_PASSWORD_KEY), "root");

    Map<String, String> gravitinoPropertiesWithoutPassword =
        ImmutableMap.of(
            "jdbc-url", "jdbc:mysql://localhost:3306",
            "jdbc-user", "root");

    Assert.assertThrows(
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

    Map<String, Object> stringObjectMap =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test connector attributes
    Assert.assertEquals(stringObjectMap.get("connectorName"), "postgresql");

    Map<String, Object> propertiesMap = (Map<String, Object>) stringObjectMap.get("properties");

    // test converted properties
    Assert.assertEquals(
        propertiesMap.get("connection-url"), "jdbc:postgresql://localhost:5432/test");
    Assert.assertEquals(propertiesMap.get("connection-user"), "test");
    Assert.assertEquals(propertiesMap.get("connection-password"), "test");

    // test trino passing properties
    Assert.assertEquals(propertiesMap.get("join-pushdown.strategy"), "EAGER");

    // test unknown properties
    Assert.assertNull(propertiesMap.get("hive.unknown-key"));
    Assert.assertNull(propertiesMap.get("trino.bypass.unknown-key"));
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

    Map<String, Object> stringObjectMap =
        adapter.buildInternalConnectorConfig(new GravitinoCatalog("test", mockCatalog));

    // test connector attributes
    Assert.assertEquals(stringObjectMap.get("connectorName"), "mysql");

    Map<String, Object> propertiesMap = (Map<String, Object>) stringObjectMap.get("properties");

    // test converted properties
    Assert.assertEquals(propertiesMap.get("connection-url"), "jdbc:mysql://localhost:5432/test");
    Assert.assertEquals(propertiesMap.get("connection-user"), "test");
    Assert.assertEquals(propertiesMap.get("connection-password"), "test");

    // test trino passing properties
    Assert.assertEquals(propertiesMap.get("join-pushdown.strategy"), "EAGER");

    // test unknown properties
    Assert.assertNull(propertiesMap.get("hive.unknown-key"));
    Assert.assertNull(propertiesMap.get("trino.bypass.unknown-key"));
  }
}
