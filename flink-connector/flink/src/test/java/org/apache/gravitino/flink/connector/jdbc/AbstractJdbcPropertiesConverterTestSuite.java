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

package org.apache.gravitino.flink.connector.jdbc;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link JdbcPropertiesConverter} */
public abstract class AbstractJdbcPropertiesConverterTestSuite {

  String username = "testUser";
  String password = "testPassword";
  String gravitinoUrl = "jdbc:mysql://192.168.1.1:3306";
  String flinkUrl = "jdbc:mysql://192.168.1.1:3306/";
  String gravitinoUrlWithParameter = "jdbc:mysql://192.168.1.1:3306/myDatabase?useSSL=false";
  String gravitinoUrlWithDomain = "jdbc:postgresql://db.example.com/myDatabase";
  String flinkUrlWithDomain = "jdbc:postgresql://db.example.com/";
  String defaultDatabase = "test";

  Map<String, String> catalogProperties;

  private static final String FLINK_BYPASS_DEFAULT_DATABASE = "flink.bypass.default-database";

  {
    catalogProperties =
        ImmutableMap.of(
            JdbcPropertiesConstants.GRAVITINO_JDBC_USER,
            username,
            JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD,
            password,
            JdbcPropertiesConstants.GRAVITINO_JDBC_URL,
            gravitinoUrl,
            FLINK_BYPASS_DEFAULT_DATABASE,
            defaultDatabase);
  }

  protected abstract JdbcPropertiesConverter getConverter(Map<String, String> catalogOptions);

  @Test
  public void testToJdbcCatalogProperties() {
    Map<String, String> properties =
        getConverter(catalogProperties).toFlinkCatalogProperties(catalogProperties);
    Assertions.assertEquals(username, properties.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(password, properties.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
    Assertions.assertEquals(flinkUrl, properties.get(JdbcPropertiesConstants.FLINK_JDBC_URL));
    Assertions.assertEquals(
        defaultDatabase, properties.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE));
  }

  @Test
  public void testToJdbcCatalogPropertiesUsingUrlWithParameter() {
    Map<String, String> catalogPropertiesUsingUrlWithParameter = new HashMap<>(catalogProperties);
    catalogPropertiesUsingUrlWithParameter.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_URL, gravitinoUrlWithParameter);
    Map<String, String> properties =
        getConverter(catalogPropertiesUsingUrlWithParameter)
            .toFlinkCatalogProperties(catalogPropertiesUsingUrlWithParameter);
    Assertions.assertEquals(username, properties.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(password, properties.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
    Assertions.assertEquals(flinkUrl, properties.get(JdbcPropertiesConstants.FLINK_JDBC_URL));
    Assertions.assertEquals(
        defaultDatabase, properties.get(JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE));
  }

  @Test
  public void testToJdbcCatalogPropertiesUsingDomainUrl() {
    Map<String, String> catalogPropertiesWithDomainUrl = new HashMap<>(catalogProperties);
    catalogPropertiesWithDomainUrl.put(
        JdbcPropertiesConstants.GRAVITINO_JDBC_URL, gravitinoUrlWithDomain);
    Map<String, String> properties =
        getConverter(catalogPropertiesWithDomainUrl)
            .toFlinkCatalogProperties(catalogPropertiesWithDomainUrl);
    Assertions.assertEquals(username, properties.get(JdbcPropertiesConstants.FLINK_JDBC_USER));
    Assertions.assertEquals(password, properties.get(JdbcPropertiesConstants.FLINK_JDBC_PASSWORD));
    Assertions.assertEquals(
        flinkUrlWithDomain, properties.get(JdbcPropertiesConstants.FLINK_JDBC_URL));
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
                flinkUrl,
                JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE,
                defaultDatabase));
    Map<String, String> properties =
        getConverter(catalogProperties).toGravitinoCatalogProperties(configuration);

    Assertions.assertEquals(username, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(
        password, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD));
    Assertions.assertEquals(flinkUrl, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_URL));
    Assertions.assertEquals(defaultDatabase, properties.get(FLINK_BYPASS_DEFAULT_DATABASE));
    Assertions.assertEquals(
        getConverter(catalogProperties).defaultDriverName(),
        properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER));
  }

  @Test
  public void testToGravitinoCatalogPropertiesWithDriver() {
    String testDriver = "testDriver";
    Configuration configuration =
        Configuration.fromMap(
            ImmutableMap.of(
                JdbcPropertiesConstants.FLINK_JDBC_USER,
                username,
                JdbcPropertiesConstants.FLINK_JDBC_PASSWORD,
                password,
                JdbcPropertiesConstants.FLINK_JDBC_URL,
                gravitinoUrl,
                JdbcPropertiesConstants.FLINK_JDBC_DEFAULT_DATABASE,
                defaultDatabase,
                JdbcPropertiesConstants.FLINK_DRIVER,
                testDriver));
    Map<String, String> properties =
        getConverter(catalogProperties).toGravitinoCatalogProperties(configuration);

    Assertions.assertEquals(username, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_USER));
    Assertions.assertEquals(
        password, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD));
    Assertions.assertEquals(
        gravitinoUrl, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_URL));
    Assertions.assertEquals(defaultDatabase, properties.get(FLINK_BYPASS_DEFAULT_DATABASE));
    Assertions.assertEquals(
        testDriver, properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER));
  }

  @Test
  public void testToJdbcCatalogPropertiesWithoutUrl() {
    Map<String, String> catalogPropertiesWithoutUrl = new HashMap<>(catalogProperties);
    catalogPropertiesWithoutUrl.remove(JdbcPropertiesConstants.GRAVITINO_JDBC_URL);

    IllegalArgumentException ex =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                getConverter(catalogPropertiesWithoutUrl)
                    .toFlinkCatalogProperties(catalogPropertiesWithoutUrl));

    Assertions.assertTrue(ex.getMessage().contains(JdbcPropertiesConstants.GRAVITINO_JDBC_URL));
  }
}
