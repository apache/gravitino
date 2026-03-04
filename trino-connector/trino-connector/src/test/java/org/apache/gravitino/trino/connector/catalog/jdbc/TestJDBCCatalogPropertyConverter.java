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

package org.apache.gravitino.trino.connector.catalog.jdbc;

import static org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_PASSWORD_KEY;
import static org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_URL_KEY;
import static org.apache.gravitino.trino.connector.catalog.jdbc.JDBCCatalogPropertyConverter.JDBC_CONNECTION_USER_KEY;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
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
  public void testMissingConnectionUrl() {
    PropertyConverter propertyConverter = new JDBCCatalogPropertyConverter();
    Map<String, String> gravitinoPropertiesWithoutUrl =
        ImmutableMap.of("jdbc-user", "root", "jdbc-password", "root");

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          propertyConverter.gravitinoToEngineProperties(gravitinoPropertiesWithoutUrl);
        });
  }
}
