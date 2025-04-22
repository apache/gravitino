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

package org.apache.gravitino.spark.connector.jdbc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.spark.connector.PropertiesConverter;

public class JdbcPropertiesConverter implements PropertiesConverter {

  private final boolean supportsTableProperties;

  public static class JdbcPropertiesConverterHolder {
    private static final JdbcPropertiesConverter INSTANCE = new JdbcPropertiesConverter();
    private static final JdbcPropertiesConverter PG_INSTANCE = new JdbcPropertiesConverter(false);
  }

  private JdbcPropertiesConverter() {
    this(true);
  }

  private JdbcPropertiesConverter(boolean supportsTableProperties) {
    this.supportsTableProperties = supportsTableProperties;
  }

  public static JdbcPropertiesConverter getInstance() {
    return JdbcPropertiesConverterHolder.INSTANCE;
  }

  public static JdbcPropertiesConverter getPGInstance() {
    return JdbcPropertiesConverterHolder.PG_INSTANCE;
  }

  private static final Map<String, String> GRAVITINO_CONFIG_TO_JDBC =
      ImmutableMap.of(
          JdbcPropertiesConstants.GRAVITINO_JDBC_URL,
          JdbcPropertiesConstants.SPARK_JDBC_URL,
          JdbcPropertiesConstants.GRAVITINO_JDBC_USER,
          JdbcPropertiesConstants.SPARK_JDBC_USER,
          JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD,
          JdbcPropertiesConstants.SPARK_JDBC_PASSWORD,
          JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER,
          JdbcPropertiesConstants.SPARK_JDBC_DRIVER);

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Jdbc Catalog properties should not be null");
    HashMap<String, String> jdbcProperties = new HashMap<>();
    properties.forEach(
        (key, value) -> {
          if (GRAVITINO_CONFIG_TO_JDBC.containsKey(key)) {
            jdbcProperties.put(GRAVITINO_CONFIG_TO_JDBC.get(key), value);
          }
        });
    return jdbcProperties;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    if (!supportsTableProperties) {
      for (String key : properties.keySet()) {
        if (!"owner".equalsIgnoreCase(key)) {
          throw new UnsupportedOperationException("Doesn't support table property " + key);
        }
      }
      return new HashMap<>();
    } else {
      return new HashMap<>(properties);
    }
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
