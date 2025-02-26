package org.apache.gravitino.flink.connector.jdbc;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link JdbcPropertiesConverter} */
public class TestJdbcPropertiesConverter {

  String username = "testUser";
  String password = "testPassword";
  String url = "testUrl";
  String defaultDatabase = "test";

  private static final JdbcPropertiesConverter CONVERTER = JdbcPropertiesConverter.INSTANCE;

  @Test
  public void testToPaimonFileSystemCatalog() {
    Map<String, String> catalogProperties =
        ImmutableMap.of(
            JdbcPropertiesConstants.GRAVITINO_JDBC_USER,
            username,
            JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD,
            password,
            JdbcPropertiesConstants.GRAVITINO_JDBC_URL,
            url,
            JdbcPropertiesConstants.GRAVITINO_JDBC_DEFAULT_DATABASE,
            defaultDatabase);
    Map<String, String> properties = CONVERTER.toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(username, properties.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(password, properties.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
    Assertions.assertEquals(url, properties.get(JdbcPropertiesConstants.FLINK_JDBC_URL));
    Assertions.assertEquals(
        defaultDatabase, properties.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE));
  }

  @Test
  public void testToGravitinoCatalogProperties() {
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                JdbcPropertiesConstants.FLINK_JDBC_USER,
                username,
                JdbcPropertiesConstants.FLINK_JDBC_PASSWORD,
                password,
                JdbcPropertiesConstants.FLINK_JDBC_URL,
                url,
                JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE,
                defaultDatabase));
    Map<String, String> properties = CONVERTER.toGravitinoCatalogProperties(configuration);

    Assertions.assertEquals(username, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(
        password, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD));
    Assertions.assertEquals(url, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_URL));
    Assertions.assertEquals(
        defaultDatabase, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DEFAULT_DATABASE));
  }
}
