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

package org.apache.gravitino.spark.connector.iceberg;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.spark.connector.PropertiesConverter;

/** Transform Apache Iceberg catalog properties between Apache Spark and Apache Gravitino. */
public class IcebergPropertiesConverter implements PropertiesConverter {

  public static class IcebergPropertiesConverterHolder {
    private static final IcebergPropertiesConverter INSTANCE = new IcebergPropertiesConverter();
  }

  private IcebergPropertiesConverter() {}

  public static IcebergPropertiesConverter getInstance() {
    return IcebergPropertiesConverter.IcebergPropertiesConverterHolder.INSTANCE;
  }

  @Override
  public Map<String, String> toSparkCatalogProperties(Map<String, String> properties) {
    Preconditions.checkArgument(
        properties != null, "Iceberg Catalog properties should not be null");

    String catalogBackend =
        properties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(catalogBackend), "Iceberg Catalog backend should not be empty.");

    HashMap<String, String> all = new HashMap<>();

    switch (catalogBackend.toLowerCase(Locale.ROOT)) {
      case IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE:
        initHiveProperties(properties, all);
        break;
      case IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC:
        initJdbcProperties(properties, all);
        break;
      case IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_BACKEND_REST:
        initRestProperties(properties, all);
        break;
      default:
        // SparkCatalog does not support Memory type catalog
        throw new IllegalArgumentException(
            "Unsupported Iceberg Catalog backend: " + catalogBackend);
    }

    all.put(IcebergPropertiesConstants.ICEBERG_CATALOG_CACHE_ENABLED, "FALSE");

    return all;
  }

  @Override
  public Map<String, String> toGravitinoTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  @Override
  public Map<String, String> toSparkTableProperties(Map<String, String> properties) {
    return new HashMap<>(properties);
  }

  private void initHiveProperties(
      Map<String, String> gravitinoProperties, HashMap<String, String> icebergProperties) {
    String metastoreUri =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metastoreUri),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI
            + " from Iceberg Catalog properties");
    String hiveWarehouse =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(hiveWarehouse),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE
            + " from Iceberg Catalog properties");
    icebergProperties.put(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_HIVE);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_URI, metastoreUri);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE, hiveWarehouse);
  }

  private void initJdbcProperties(
      Map<String, String> gravitinoProperties, HashMap<String, String> icebergProperties) {
    String jdbcUri =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcUri),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI
            + " from Iceberg Catalog properties");
    String jdbcWarehouse =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcWarehouse),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE
            + " from Iceberg Catalog properties");
    String jdbcUser =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcUser),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_USER
            + " from Iceberg Catalog properties");
    String jdbcPassword =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(jdbcPassword),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD
            + " from Iceberg Catalog properties");
    icebergProperties.put(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_JDBC);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_URI, jdbcUri);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE, jdbcWarehouse);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_USER, jdbcUser);
    icebergProperties.put(IcebergPropertiesConstants.ICEBERG_CATALOG_JDBC_PASSWORD, jdbcPassword);
  }

  private void initRestProperties(
      Map<String, String> gravitinoProperties, HashMap<String, String> icebergProperties) {
    String restUri =
        gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(restUri),
        "Couldn't get "
            + IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI
            + " from Iceberg Catalog properties");
    icebergProperties.put(
        IcebergPropertiesConstants.ICEBERG_CATALOG_TYPE,
        IcebergPropertiesConstants.ICEBERG_CATALOG_BACKEND_REST);
    icebergProperties.put(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_URI, restUri);
    if (gravitinoProperties.containsKey(
        IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE)) {
      icebergProperties.put(
          IcebergPropertiesConstants.ICEBERG_CATALOG_WAREHOUSE,
          gravitinoProperties.get(IcebergPropertiesConstants.GRAVITINO_ICEBERG_CATALOG_WAREHOUSE));
    }
  }
}
