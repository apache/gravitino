/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.trino.connector.catalog.jdbc;

import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_URL_KEY;
import static com.datastrato.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY;

import com.datastrato.gravitino.catalog.property.PropertyConverter;
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
}
