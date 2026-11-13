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

package org.apache.gravitino.spark.connector.paimon;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;
import org.apache.gravitino.catalog.lakehouse.paimon.PaimonPropertiesUtils;
import org.apache.gravitino.spark.connector.PropertiesConverter;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class PaimonPropertiesConverter implements PropertiesConverter {

  public static class PaimonPropertiesConverterHolder {
    private static final PaimonPropertiesConverter INSTANCE = new PaimonPropertiesConverter();
  }

  private PaimonPropertiesConverter() {}

  public static PaimonPropertiesConverter getInstance() {
    return PaimonPropertiesConverter.PaimonPropertiesConverterHolder.INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(properties != null, "Paimon Catalog properties should not be null");
    Map<String, String> all = PaimonPropertiesUtils.toPaimonCatalogProperties(properties);
    String catalogBackend = all.remove(PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogBackend),
        String.format(
            "%s should not be empty", PaimonPropertiesConstants.GRAVITINO_PAIMON_CATALOG_BACKEND));
    all.put(PaimonPropertiesConstants.PAIMON_CATALOG_METASTORE, catalogBackend);
    return all;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    HashMap<String, String> gravitinoTableProperties = new HashMap<>(properties);
    // The owner property of Paimon is a reserved property, so we need to remove it.
    gravitinoTableProperties.remove(PaimonConstants.OWNER);
    return gravitinoTableProperties;
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  /**
   * Converts the Spark table properties of a table created in a Paimon catalog with the Hive
   * backend, the only Paimon backend that can create format tables. It follows Paimon's own
   * SparkCatalog: a file format provider (csv, json, orc, parquet, ...) from the USING clause makes
   * a format table of that format, "paimon" or no provider keeps a Paimon table, and LOCATION
   * becomes the Paimon table path. Other backends keep the properties as they are.
   *
   * @param properties the Spark table properties, already converted by {@link
   *     #toGravitinoTableProperties(Map)}.
   * @return the table properties to pass to the Hive backend.
   */
  Map<String, String> toHiveBackendTableProperties(Map<String, String> properties) {
    Map<String, String> tableProperties = new HashMap<>(properties);
    String provider = tableProperties.remove(TableCatalog.PROP_PROVIDER);
    if (StringUtils.isNotBlank(provider)
        && !PaimonPropertiesConstants.PAIMON_PROVIDER.equalsIgnoreCase(provider)) {
      tableProperties.put(
          PaimonPropertiesConstants.PAIMON_TABLE_TYPE,
          PaimonPropertiesConstants.PAIMON_TABLE_TYPE_FORMAT_TABLE);
      tableProperties.put(
          PaimonPropertiesConstants.PAIMON_FILE_FORMAT, provider.toLowerCase(Locale.ROOT));
    }
    String location = tableProperties.remove(TableCatalog.PROP_LOCATION);
    if (StringUtils.isNotBlank(location)) {
      tableProperties.put(PaimonPropertiesConstants.PAIMON_TABLE_LOCATION, location);
    }
    // Spark marks a table with a LOCATION as external; Paimon decides that from the path itself.
    tableProperties.remove(TableCatalog.PROP_EXTERNAL);
    return tableProperties;
  }
}
