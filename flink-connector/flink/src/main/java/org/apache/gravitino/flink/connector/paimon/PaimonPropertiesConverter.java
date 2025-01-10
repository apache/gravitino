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

package org.apache.gravitino.flink.connector.paimon;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonPropertiesUtils;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.paimon.catalog.FileSystemCatalogFactory;

public class PaimonPropertiesConverter implements PropertiesConverter {

  public static final PaimonPropertiesConverter INSTANCE = new PaimonPropertiesConverter();

  private PaimonPropertiesConverter() {}

  @Override
  public Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    Map<String, String> gravitinoProperties = Maps.newHashMap();
    Map<String, String> flinkConfMap = flinkConf.toMap();
    for (Map.Entry<String, String> entry : flinkConfMap.entrySet()) {
      String gravitinoKey =
          PaimonPropertiesUtils.PAIMON_CATALOG_CONFIG_TO_GRAVITINO.get(entry.getKey());
      if (gravitinoKey != null) {
        gravitinoProperties.put(gravitinoKey, entry.getValue());
      } else if (!entry.getKey().startsWith(FLINK_PROPERTY_PREFIX)) {
        gravitinoProperties.put(FLINK_PROPERTY_PREFIX + entry.getKey(), entry.getValue());
      } else {
        gravitinoProperties.put(entry.getKey(), entry.getValue());
      }
    }
    gravitinoProperties.put(
        PaimonConstants.CATALOG_BACKEND,
        flinkConfMap.getOrDefault(PaimonConstants.METASTORE, FileSystemCatalogFactory.IDENTIFIER));
    return gravitinoProperties;
  }

  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> flinkCatalogProperties = Maps.newHashMap();
    flinkCatalogProperties.put(
        CommonCatalogOptions.CATALOG_TYPE.key(), GravitinoPaimonCatalogFactoryOptions.IDENTIFIER);
    gravitinoProperties.forEach(
        (key, value) -> {
          String flinkConfigKey = key;
          if (key.startsWith(PropertiesConverter.FLINK_PROPERTY_PREFIX)) {
            flinkConfigKey = key.substring(PropertiesConverter.FLINK_PROPERTY_PREFIX.length());
          }
          flinkCatalogProperties.put(
              PaimonPropertiesUtils.GRAVITINO_CONFIG_TO_PAIMON.getOrDefault(
                  flinkConfigKey, flinkConfigKey),
              value);
        });

    return flinkCatalogProperties;
  }
}
