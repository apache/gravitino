/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.hadoop.hive.conf.HiveConf;

public class HivePropertiesConverter implements PropertiesConverter {

  private HivePropertiesConverter() {}

  public static final HivePropertiesConverter INSTANCE = new HivePropertiesConverter();

  private static final Map<String, String> HIVE_CONFIG_TO_GRAVITINO =
      ImmutableMap.of(HiveConf.ConfVars.METASTOREURIS.varname, METASTORE_URIS);
  private static final Map<String, String> GRAVITINO_CONFIG_TO_HIVE =
      ImmutableMap.of(METASTORE_URIS, HiveConf.ConfVars.METASTOREURIS.varname);

  @Override
  public Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    Map<String, String> gravitinoProperties = Maps.newHashMap();

    for (Map.Entry<String, String> entry : flinkConf.toMap().entrySet()) {
      String hiveKey = HIVE_CONFIG_TO_GRAVITINO.get(entry.getKey());
      if (hiveKey != null) {
        gravitinoProperties.put(hiveKey, entry.getValue());
      } else if (!entry.getKey().startsWith(FLINK_PROPERTY_PREFIX)) {
        gravitinoProperties.put(FLINK_PROPERTY_PREFIX + entry.getKey(), entry.getValue());
      } else {
        gravitinoProperties.put(entry.getKey(), entry.getValue());
      }
    }

    return gravitinoProperties;
  }

  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> flinkCatalogProperties = Maps.newHashMap();
    flinkCatalogProperties.put(
        CommonCatalogOptions.CATALOG_TYPE.key(), GravitinoHiveCatalogFactoryOptions.IDENTIFIER);

    gravitinoProperties.forEach(
        (key, value) -> {
          String flinkConfigKey = key;
          if (key.startsWith(PropertiesConverter.FLINK_PROPERTY_PREFIX)) {
            flinkConfigKey = key.substring(PropertiesConverter.FLINK_PROPERTY_PREFIX.length());
          }
          flinkCatalogProperties.put(
              GRAVITINO_CONFIG_TO_HIVE.getOrDefault(flinkConfigKey, flinkConfigKey), value);
        });
    return flinkCatalogProperties;
  }
}
