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

package org.apache.gravitino.spark.connector;

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Interface for transforming properties between Apache Gravitino and Apache Spark. */
public interface PropertiesConverter {
  @VisibleForTesting String SPARK_PROPERTY_PREFIX = "spark.bypass.";

  /**
   * Converts properties from application provided properties and Gravitino catalog properties to
   * Spark connector properties.
   *
   * <p>It provides the common implementation, include extract properties with "spark.bypass"
   * prefix, merge user provided options and transformed properties.
   *
   * @param options Case-insensitive properties map provided by application configuration.
   * @param properties Gravitino catalog properties.
   * @return properties for the Spark connector.
   */
  default Map<String, String> toSparkCatalogProperties(
      CaseInsensitiveStringMap options, Map<String, String> properties) {
    Map<String, String> all = new HashMap<>();
    if (properties != null) {
      properties.forEach(
          (k, v) -> {
            if (k.startsWith(SPARK_PROPERTY_PREFIX)) {
              String newKey = k.substring(SPARK_PROPERTY_PREFIX.length());
              all.put(newKey, v);
            }
          });
    }

    Map<String, String> transformedProperties = toSparkCatalogProperties(properties);
    if (transformedProperties != null) {
      all.putAll(transformedProperties);
    }

    if (options != null) {
      all.putAll(options);
    }
    return all;
  }

  /**
   * Transform properties from Gravitino catalog properties to Spark connector properties.
   *
   * <p>This interface focuses on the catalog specific transform logic, the common logic are
   * implemented in {@code toSparkCatalogProperties}.
   *
   * @param properties Gravitino catalog properties.
   * @return properties for the Spark connector.
   */
  Map<String, String> toSparkCatalogProperties(Map<String, String> properties);

  /**
   * Converts Spark table properties to Gravitino table properties.
   *
   * @param properties Spark table properties.
   * @return Gravitino table properties.
   */
  Map<String, String> toGravitinoTableProperties(Map<String, String> properties);

  /**
   * Converts Gravitino table properties to Spark table properties.
   *
   * @param properties Gravitino table properties.
   * @return Spark table properties.
   */
  Map<String, String> toSparkTableProperties(Map<String, String> properties);
}
