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

package org.apache.gravitino.spark.connector.jdbc.doris;

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.gravitino.spark.connector.jdbc.JdbcPropertiesConverter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Converts catalog-managed properties for the specialized Doris read path. */
final class DorisPropertiesConverter35 implements PropertiesConverter {

  private static final DorisPropertiesConverter35 INSTANCE = new DorisPropertiesConverter35();
  private static final Set<String> PROTECTED_OPTIONS =
      ImmutableSet.of(
          "url",
          "user",
          "password",
          "driver",
          "jdbc-url",
          "jdbc-user",
          "jdbc-password",
          "jdbc-driver");

  private DorisPropertiesConverter35() {}

  static DorisPropertiesConverter35 getInstance() {
    return INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(
      CaseInsensitiveStringMap options, Map<String, String> properties) {
    Map<String, String> converted =
        new HashMap<>(JdbcPropertiesConverter.getInstance().toSparkCatalogProperties(properties));
    // Specialized mode accepts only the credential vended by Gravitino. The catalog-level
    // jdbc-user and jdbc-password remain available to the generic JDBC path.
    converted.remove("user");
    converted.remove("password");
    if (options != null) {
      for (Map.Entry<String, String> option : options.entrySet()) {
        String key = option.getKey();
        if (PROTECTED_OPTIONS.contains(key.toLowerCase(Locale.ROOT))) {
          throw new IllegalArgumentException(
              "Doris specialized connection options must be catalog-managed: " + key);
        }
        converted.put(key, option.getValue());
      }
    }
    return converted;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    return toSparkCatalogProperties(null, properties);
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
