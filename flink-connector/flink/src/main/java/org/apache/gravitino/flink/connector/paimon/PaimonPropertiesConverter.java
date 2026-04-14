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

import java.util.HashMap;
import java.util.Map;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonPropertiesUtils;
import org.apache.gravitino.flink.connector.CatalogPropertiesConverter;
import org.apache.gravitino.flink.connector.SchemaAndTablePropertiesConverter;

public class PaimonPropertiesConverter
    implements CatalogPropertiesConverter, SchemaAndTablePropertiesConverter {

  public static final PaimonPropertiesConverter INSTANCE = new PaimonPropertiesConverter();

  private PaimonPropertiesConverter() {}

  @Override
  public String transformPropertyToGravitinoCatalog(String configKey) {
    if (configKey.equalsIgnoreCase(PaimonConstants.METASTORE)) {
      return PaimonConstants.CATALOG_BACKEND;
    }
    return PaimonPropertiesUtils.PAIMON_CATALOG_CONFIG_TO_GRAVITINO.get(configKey);
  }

  @Override
  public String transformPropertyToFlinkCatalog(String configKey) {
    if (configKey.equals(PaimonConstants.CATALOG_BACKEND)) {
      return PaimonConstants.METASTORE;
    }
    return PaimonPropertiesUtils.GRAVITINO_CONFIG_TO_PAIMON.get(configKey);
  }

  /**
   * Merges translated catalog-level properties (e.g. {@code metastore=hive}, {@code uri}, {@code
   * warehouse}) into the table properties so that {@link org.apache.paimon.flink.FlinkTableFactory}
   * can construct the correct {@code MetastoreClient} (e.g. {@code HiveMetastoreClient}) during
   * data-write commits. Without this merge, partition metadata is never synced to Hive metastore
   * when writing to partitioned Paimon tables.
   *
   * <p>The catalog options may arrive in two forms depending on how the catalog was constructed:
   *
   * <ul>
   *   <li><b>Gravitino-side keys</b> (e.g. {@code catalog-backend=hive}) — translated to Paimon
   *       keys via {@link #transformPropertyToFlinkCatalog}.
   *   <li><b>Paimon-native keys</b> (e.g. {@code metastore=hive}) — detected by a reverse lookup
   *       via {@link #transformPropertyToGravitinoCatalog} and kept as-is.
   * </ul>
   *
   * <p>Table-level properties take precedence over catalog-level properties.
   */
  @Override
  public Map<String, String> toFlinkTableProperties(
      Map<String, String> flinkCatalogProperties,
      Map<String, String> gravitinoTableProperties,
      ObjectPath tablePath) {
    Map<String, String> result = new HashMap<>(gravitinoTableProperties);
    for (Map.Entry<String, String> entry : flinkCatalogProperties.entrySet()) {
      String paimonKey = transformPropertyToFlinkCatalog(entry.getKey());
      if (paimonKey == null && transformPropertyToGravitinoCatalog(entry.getKey()) != null) {
        // The key is already in Paimon-native format (e.g. "metastore", "uri", "warehouse").
        paimonKey = entry.getKey();
      }
      if (paimonKey != null) {
        // Table-level properties win; catalog options fill in only if not already set.
        result.putIfAbsent(paimonKey, entry.getValue());
      }
    }
    return result;
  }

  @Override
  public String getFlinkCatalogType() {
    return GravitinoPaimonCatalogFactoryOptions.IDENTIFIER;
  }
}
