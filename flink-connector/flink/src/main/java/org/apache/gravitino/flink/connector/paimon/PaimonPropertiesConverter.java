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
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogBackend;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConfig;
import org.apache.gravitino.flink.connector.PropertiesConverter;
import org.apache.paimon.options.CatalogOptions;

public class PaimonPropertiesConverter implements PropertiesConverter {

  public static final PaimonPropertiesConverter INSTANCE = new PaimonPropertiesConverter();

  private PaimonPropertiesConverter() {}

  @Override
  public Map<String, String> toGravitinoCatalogProperties(Configuration flinkConf) {
    Map<String, String> gravitinoCatalogProperties = flinkConf.toMap();
    String warehouse = flinkConf.get(GravitinoPaimonCatalogFactoryOptions.WAREHOUSE);
    gravitinoCatalogProperties.put(PaimonConfig.CATALOG_WAREHOUSE.getKey(), warehouse);
    String backendType = flinkConf.get(GravitinoPaimonCatalogFactoryOptions.CATALOG_BACKEND);
    gravitinoCatalogProperties.put(
        PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND, backendType);
    if (PaimonCatalogBackend.JDBC.name().equalsIgnoreCase(backendType)) {
      gravitinoCatalogProperties.put(
          PaimonConfig.CATALOG_URI.getKey(),
          flinkConf.get(GravitinoPaimonCatalogFactoryOptions.URI));
      gravitinoCatalogProperties.put(
          PaimonConfig.CATALOG_JDBC_USER.getKey(),
          flinkConf.get(GravitinoPaimonCatalogFactoryOptions.JDBC_USER));
      gravitinoCatalogProperties.put(
          PaimonConfig.CATALOG_JDBC_PASSWORD.getKey(),
          flinkConf.get(GravitinoPaimonCatalogFactoryOptions.JDBC_PASSWORD));
    } else if (PaimonCatalogBackend.HIVE.name().equalsIgnoreCase(backendType)) {
      throw new UnsupportedOperationException(
          "The Gravitino Connector does not currently support creating a Paimon Catalog that uses Hive Metastore.");
    }
    return gravitinoCatalogProperties;
  }

  @Override
  public Map<String, String> toFlinkCatalogProperties(Map<String, String> gravitinoProperties) {
    Map<String, String> flinkCatalogProperties = Maps.newHashMap();
    flinkCatalogProperties.putAll(gravitinoProperties);
    String backendType =
        flinkCatalogProperties.get(PaimonCatalogPropertiesMetadata.GRAVITINO_CATALOG_BACKEND);
    if (PaimonCatalogBackend.JDBC.name().equalsIgnoreCase(backendType)) {
      flinkCatalogProperties.put(CatalogOptions.METASTORE.key(), backendType);
      flinkCatalogProperties.put(
          GravitinoPaimonCatalogFactoryOptions.URI.key(),
          gravitinoProperties.get(PaimonConfig.CATALOG_URI.getKey()));
      flinkCatalogProperties.put(
          GravitinoPaimonCatalogFactoryOptions.JDBC_USER.key(),
          gravitinoProperties.get(PaimonConfig.CATALOG_JDBC_USER.getKey()));
      flinkCatalogProperties.put(
          GravitinoPaimonCatalogFactoryOptions.JDBC_PASSWORD.key(),
          gravitinoProperties.get(PaimonConfig.CATALOG_JDBC_PASSWORD.getKey()));
    } else if (PaimonCatalogBackend.HIVE.name().equalsIgnoreCase(backendType)) {
      throw new UnsupportedOperationException(
          "The Gravitino Connector does not currently support creating a Paimon Catalog that uses Hive Metastore.");
    }
    flinkCatalogProperties.put(
        GravitinoPaimonCatalogFactoryOptions.CATALOG_BACKEND.key(), backendType);
    flinkCatalogProperties.put(
        CommonCatalogOptions.CATALOG_TYPE.key(), GravitinoPaimonCatalogFactoryOptions.IDENTIFIER);
    return flinkCatalogProperties;
  }
}
