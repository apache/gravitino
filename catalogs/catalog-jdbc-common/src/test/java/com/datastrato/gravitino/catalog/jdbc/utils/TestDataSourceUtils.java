/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.jdbc.utils;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import com.google.common.collect.Maps;
import java.sql.SQLException;
import java.util.HashMap;
import javax.sql.DataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDataSourceUtils {

  @Test
  public void testCreateDataSource() throws SQLException {
    HashMap<String, String> properties = Maps.newHashMap();
    properties.put(JdbcConfig.JDBC_DRIVER.getKey(), "org.sqlite.JDBC");
    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:sqlite::memory:");
    properties.put(JdbcConfig.USERNAME.getKey(), "test");
    properties.put(JdbcConfig.PASSWORD.getKey(), "test");

    DataSource dataSource =
        Assertions.assertDoesNotThrow(() -> DataSourceUtils.createDataSource(properties));
    Assertions.assertTrue(dataSource instanceof org.apache.commons.dbcp2.BasicDataSource);
    ((BasicDataSource) dataSource).close();
  }
}
