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

package org.apache.gravitino.flink.connector.hive;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.gravitino.catalog.hive.HiveConstants;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.hadoop.hive.conf.HiveConf;

public class HivePropertiesConverter implements PropertiesConverter {

  private HivePropertiesConverter() {}

  public static final HivePropertiesConverter INSTANCE = new HivePropertiesConverter();
  private static final Map<String, String> HIVE_CATALOG_CONFIG_TO_GRAVITINO =
      ImmutableMap.of(HiveConf.ConfVars.METASTOREURIS.varname, HiveConstants.METASTORE_URIS);
  private static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(HiveConstants.METASTORE_URIS, HiveConf.ConfVars.METASTOREURIS.varname);

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    return HIVE_CATALOG_CONFIG_TO_GRAVITINO.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    return GRAVITINO_CONFIG_TO_HIVE.get(configKey);
  }

  @Override
  public Map<String, String> toFlinkTableProperties(
      Map<String, String> flinkCatalogProperties,
      Map<String, String> gravitinoTableProperties,
      ObjectPath tablePath) {
    Map<String, String> properties =
        gravitinoTableProperties.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> {
                      String key = entry.getKey();
                      if (key.startsWith(HiveConstants.SERDE_PARAMETER_PREFIX)) {
                        return key.substring(HiveConstants.SERDE_PARAMETER_PREFIX.length());
                      } else {
                        return key;
                      }
                    },
                    Map.Entry::getValue,
                    (existingValue, newValue) -> newValue));
    properties.put("connector", "hive");
    return properties;
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoHiveCatalogFactoryOptions.IDENTIFIER;
  }
}
