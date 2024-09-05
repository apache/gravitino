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

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;

public class IcebergPropertiesConstants {
  @VisibleForTesting
  public static final String GRAVITINO_ICEBERG_CATALOG_BACKEND = IcebergConstants.CATALOG_BACKEND;

  static final String ICEBERG_CATALOG_TYPE = CatalogUtil.ICEBERG_CATALOG_TYPE;

  public static final String GRAVITINO_ICEBERG_CATALOG_WAREHOUSE = IcebergConstants.WAREHOUSE;

  static final String ICEBERG_CATALOG_WAREHOUSE = CatalogProperties.WAREHOUSE_LOCATION;

  public static final String GRAVITINO_ICEBERG_CATALOG_URI = IcebergConstants.URI;

  static final String ICEBERG_CATALOG_URI = CatalogProperties.URI;

  static final String GRAVITINO_ICEBERG_CATALOG_JDBC_USER = IcebergConstants.GRAVITINO_JDBC_USER;
  static final String ICEBERG_CATALOG_JDBC_USER = IcebergConstants.ICEBERG_JDBC_USER;

  static final String GRAVITINO_ICEBERG_CATALOG_JDBC_PASSWORD =
      IcebergConstants.GRAVITINO_JDBC_PASSWORD;
  static final String ICEBERG_CATALOG_JDBC_PASSWORD = IcebergConstants.ICEBERG_JDBC_PASSWORD;

  @VisibleForTesting
  public static final String ICEBERG_CATALOG_BACKEND_HIVE = CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE = "hive";

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_JDBC = "jdbc";
  static final String ICEBERG_CATALOG_BACKEND_JDBC = "jdbc";

  @VisibleForTesting
  public static final String ICEBERG_CATALOG_BACKEND_REST = CatalogUtil.ICEBERG_CATALOG_TYPE_REST;

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_REST = "rest";

  @VisibleForTesting public static final String ICEBERG_LOCATION = IcebergConstants.LOCATION;

  @VisibleForTesting
  public static final String ICEBERG_CURRENT_SNAPSHOT_ID = IcebergConstants.CURRENT_SNAPSHOT_ID;

  @VisibleForTesting public static final String ICEBERG_SORT_ORDER = IcebergConstants.SORT_ORDER;

  @VisibleForTesting
  public static final String ICEBERG_IDENTIFIER_FIELDS = IcebergConstants.IDENTIFIER_FIELDS;

  @VisibleForTesting public static final String ICEBERG_PROVIDER = IcebergConstants.PROVIDER;

  @VisibleForTesting public static final String ICEBERG_FILE_FORMAT = IcebergConstants.FORMAT;

  @VisibleForTesting
  public static final String ICEBERG_FORMAT_VERSION = IcebergConstants.FORMAT_VERSION;

  @VisibleForTesting
  public static final String ICEBERG_CATALOG_CACHE_ENABLED = CatalogProperties.CACHE_ENABLED;

  static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_NAME =
      IcebergConstants.CATALOG_BACKEND_NAME;

  private IcebergPropertiesConstants() {}
}
