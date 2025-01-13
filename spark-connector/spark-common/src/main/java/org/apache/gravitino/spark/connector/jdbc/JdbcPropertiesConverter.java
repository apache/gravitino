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
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.spark.connector.PropertiesConverter;

public class JdbcPropertiesConverter implements PropertiesConverter {

  public static class JdbcPropertiesConverterHolder {
    private static final JdbcPropertiesConverter INSTANCE = new JdbcPropertiesConverter();
  }

  private JdbcPropertiesConverter() {}

  public static JdbcPropertiesConverter getInstance() {
    return JdbcPropertiesConverterHolder.INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Jdbc Catalog properties should not be null");
    HashMap<String, String> jdbcProperties = new HashMap<>();
    jdbcProperties.put(
        JdbcPropertiesConstants.SPARK_JDBC_URL,
        properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_URL));
    jdbcProperties.put(
        JdbcPropertiesConstants.SPARK_JDBC_USER,
        properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_USER));
    jdbcProperties.put(
        JdbcPropertiesConstants.SPARK_JDBC_PASSWORD,
        properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_PASSWORD));
    jdbcProperties.put(
        JdbcPropertiesConstants.SPARK_JDBC_DRIVER,
        properties.get(JdbcPropertiesConstants.GRAVITINO_JDBC_DRIVER));
    return jdbcProperties;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }
}
