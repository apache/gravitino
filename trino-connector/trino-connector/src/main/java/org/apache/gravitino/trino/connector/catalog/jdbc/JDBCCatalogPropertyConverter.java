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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections4.bidimap.TreeBidiMap;
import org.apache.gravitino.trino.connector.catalog.CatalogPropertyConverter;

/**
 * Property converter for JDBC catalog properties. Handles the conversion of property keys between
 * Trino and Gravitino formats for JDBC catalogs, including connection, authentication, and general
 * configuration properties.
 */
public class JDBCCatalogPropertyConverter extends CatalogPropertyConverter {

  /** Property key for JDBC connection URL. */
  public static final String JDBC_CONNECTION_URL_KEY = "connection-url";

  /** Property key for JDBC connection username. */
  public static final String JDBC_CONNECTION_USER_KEY = "connection-user";

  /** Property key for JDBC connection password. */
  public static final String JDBC_CONNECTION_PASSWORD_KEY = "connection-password";

  private static final TreeBidiMap<String, String> TRINO_KEY_TO_GRAVITINO_KEY =
      new TreeBidiMap<>(
          new ImmutableMap.Builder<String, String>()
              // Key is the Trino property, value is the Gravitino property
              .put(JDBC_CONNECTION_URL_KEY, "jdbc-url")

              // Data source authentication
              .put(JDBC_CONNECTION_USER_KEY, "jdbc-user")
              .put(JDBC_CONNECTION_PASSWORD_KEY, "jdbc-password")
              .build());

  /** Set of required properties for JDBC connection. */
  public static final Set<String> REQUIRED_PROPERTIES =
      Sets.newHashSet(
          JDBC_CONNECTION_URL_KEY, JDBC_CONNECTION_USER_KEY, JDBC_CONNECTION_PASSWORD_KEY);

  @Override
  public TreeBidiMap<String, String> engineToGravitinoMapping() {
    return TRINO_KEY_TO_GRAVITINO_KEY;
  }

  @Override
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> properties) {
    Map<String, String> trinoProperties = super.gravitinoToEngineProperties(properties);
    for (String requiredProperty : REQUIRED_PROPERTIES) {
      if (!trinoProperties.containsKey(requiredProperty)) {
        throw new IllegalArgumentException("Missing required property: " + requiredProperty);
      }
    }

    return trinoProperties;
  }
}
