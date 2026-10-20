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
package org.apache.gravitino.catalog;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;

/** Adds the known JDBC driver class to catalog properties when the caller omits it. */
public final class JdbcDriverProperties {

  private static final String JDBC_DRIVER = "jdbc-driver";
  private static final Map<String, String> PROVIDER_DRIVERS =
      ImmutableMap.<String, String>builder()
          .put("jdbc-clickhouse", "com.clickhouse.jdbc.ClickHouseDriver")
          .put("jdbc-doris", "com.mysql.cj.jdbc.Driver")
          .put("jdbc-hologres", "org.postgresql.Driver")
          .put("jdbc-mysql", "com.mysql.cj.jdbc.Driver")
          .put("jdbc-postgresql", "org.postgresql.Driver")
          .put("jdbc-starrocks", "com.mysql.cj.jdbc.Driver")
          .build();

  private JdbcDriverProperties() {}

  /**
   * Returns properties with a derived driver, leaving the input map unchanged.
   *
   * @param provider catalog provider
   * @param properties requested catalog properties
   * @return properties with a driver when one can be inferred
   */
  @Nullable
  public static Map<String, String> withDefaultDriver(
      @Nullable String provider, @Nullable Map<String, String> properties) {
    if (provider == null || (properties != null && properties.get(JDBC_DRIVER) != null)) {
      return properties;
    }

    String driver = PROVIDER_DRIVERS.get(provider.toLowerCase(Locale.ROOT));
    if ("jdbc-oceanbase".equalsIgnoreCase(provider)) {
      driver = null;
      String url = properties == null ? null : properties.get("jdbc-url");
      if (url != null) {
        String normalizedUrl = url.trim().toLowerCase(Locale.ROOT);
        if (normalizedUrl.startsWith("jdbc:oceanbase:")) {
          driver = "com.oceanbase.jdbc.Driver";
        } else if (normalizedUrl.startsWith("jdbc:mysql:")) {
          driver = "com.mysql.cj.jdbc.Driver";
        }
      }
    }
    if (driver == null && "lakehouse-iceberg".equalsIgnoreCase(provider) && properties != null) {
      if ("jdbc".equalsIgnoreCase(properties.get("catalog-backend"))) {
        String uri = properties.get("uri");
        if (uri != null) {
          String normalizedUri = uri.trim().toLowerCase(Locale.ROOT);
          if (normalizedUri.startsWith("jdbc:mysql:")) {
            driver = "com.mysql.cj.jdbc.Driver";
          } else if (normalizedUri.startsWith("jdbc:postgresql:")) {
            driver = "org.postgresql.Driver";
          }
        }
      }
    }

    if (driver == null) {
      return properties;
    }
    Map<String, String> result = new HashMap<>();
    if (properties != null) {
      result.putAll(properties);
    }
    result.put(JDBC_DRIVER, driver);
    return result;
  }
}
