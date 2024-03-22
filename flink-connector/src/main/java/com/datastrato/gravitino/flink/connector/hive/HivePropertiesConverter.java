/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.hive;

import static com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta.METASTORE_URIS;

import com.datastrato.gravitino.flink.connector.PropertiesConverter;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.factories.HiveCatalogFactoryOptions;
import org.apache.hadoop.hive.conf.HiveConf;

public class HivePropertiesConverter implements PropertiesConverter {

  private HivePropertiesConverter() {}

  public static final HivePropertiesConverter INSTANCE = new HivePropertiesConverter();

  @Override
  public Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    Map<String, String> gravitinoProperties = Maps.newHashMap();

    // set metastore.uris
    String hiveConfDir = flinkConf.get(HiveCatalogFactoryOptions.HIVE_CONF_DIR);
    HiveConf hiveConf = HiveCatalog.createHiveConf(hiveConfDir, null);
    String metastoreUris =
        Preconditions.checkNotNull(
            hiveConf.get(HiveConf.ConfVars.METASTOREURIS.varname),
            "%s should not be null",
            HiveConf.ConfVars.METASTOREURIS.varname);
    gravitinoProperties.put(METASTORE_URIS, metastoreUris);

    for (Map.Entry<String, String> entry : flinkConf.toMap().entrySet()) {
      if (!entry.getKey().startsWith(FLINK_PROPERTY_PREFIX)) {
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
        CommonCatalogOptions.CATALOG_TYPE.key(), GravitinoHiveCatalogFactory.IDENTIFIER);

    gravitinoProperties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(FLINK_PROPERTY_PREFIX))
        .forEach(
            e ->
                flinkCatalogProperties.put(
                    e.getKey().substring(PropertiesConverter.FLINK_PROPERTY_PREFIX.length()),
                    e.getValue()));

    return flinkCatalogProperties;
  }
}
