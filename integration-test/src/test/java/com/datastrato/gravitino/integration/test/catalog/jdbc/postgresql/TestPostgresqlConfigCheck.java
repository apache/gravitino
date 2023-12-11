package com.datastrato.gravitino.integration.test.catalog.jdbc.postgresql;

import com.datastrato.gravitino.catalog.jdbc.config.JdbcConfig;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPostgresqlConfigCheck {

  @Test
  public void checkDatabase() {
    Map<String, String> properties = new HashMap<>();
    properties.put(
        JdbcConfig.JDBC_URL.getKey(), "jdbc:postgresql://localhost:60440/test_db_9728?123");
    String database =
        Assertions.assertDoesNotThrow(
            () -> new JdbcConfig(properties).getJdbcDatabaseOrElseThrow("have database"));
    Assertions.assertEquals("test_db_9728", database);

    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:postgresql://localhost:60440/test_db_9728");
    database =
        Assertions.assertDoesNotThrow(
            () -> new JdbcConfig(properties).getJdbcDatabaseOrElseThrow("have database"));
    Assertions.assertEquals("test_db_9728", database);

    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:postgresql://localhost:60440");
    IllegalArgumentException illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new JdbcConfig(properties).getJdbcDatabaseOrElseThrow("no database"));
    Assertions.assertEquals("no database", illegalArgumentException.getMessage());

    properties.put(JdbcConfig.JDBC_URL.getKey(), "jdbc:postgresql://localhost:60440/");
    illegalArgumentException =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> new JdbcConfig(properties).getJdbcDatabaseOrElseThrow("no database"));
    Assertions.assertEquals("no database", illegalArgumentException.getMessage());
  }
}
