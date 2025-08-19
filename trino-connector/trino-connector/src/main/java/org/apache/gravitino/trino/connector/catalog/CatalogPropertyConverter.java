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

package org.apache.gravitino.trino.connector.catalog;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.catalog.property.PropertyConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogPropertyConverter extends PropertyConverter {
  private static final Logger LOG = LoggerFactory.getLogger(PropertyConverter.class);

  private static final String TRINO_PROPERTIES_PREFIX = "trino.bypass.";

  @Override
  public Map<String, String> engineToGravitinoMapping() {
    return ImmutableMap.of();
  }

  /**
   * Convert Gravitino properties to engine properties. Support skip validation and directly pass
   * some config through
   *
   * @param gravitinoProperties map of Gravitino properties
   * @return map of engine properties
   */
  @Override
  public Map<String, String> gravitinoToEngineProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> engineProperties = new HashMap<>();
    Map<String, String> gravitinoToEngineMapping = reverseMap(engineToGravitinoMapping());
    Map<String, String> trinoBypassProperties = new HashMap<>();
    for (Map.Entry<String, String> entry : gravitinoProperties.entrySet()) {
      String gravitinoKey = entry.getKey();
      if (gravitinoKey.startsWith(TRINO_PROPERTIES_PREFIX)) {
        trinoBypassProperties.put(
            gravitinoKey.replace(TRINO_PROPERTIES_PREFIX, ""), entry.getValue());
        continue;
      }
      String engineKey = gravitinoToEngineMapping.get(gravitinoKey);
      if (engineKey != null) {
        engineProperties.put(engineKey, entry.getValue());
      } else {
        LOG.info("Property {} is not supported by engine", entry.getKey());
      }
    }
    // trino.bypass properties will be skipped when the catalog properties is defined by Gravitino
    if (!trinoBypassProperties.isEmpty()) {
      for (Map.Entry<String, String> entry : trinoBypassProperties.entrySet()) {
        String key = entry.getKey();
        if (!engineProperties.containsKey(key)) {
          engineProperties.put(key, entry.getValue());
        } else {
          LOG.info("Property {} which with trino.bypass prefix is skipped", key);
        }
      }
    }
    return engineProperties;
  }
}
