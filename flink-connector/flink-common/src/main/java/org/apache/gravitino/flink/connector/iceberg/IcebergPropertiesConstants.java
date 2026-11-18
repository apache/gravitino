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

package org.apache.gravitino.flink.connector.iceberg;

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.flink.FlinkCatalogFactory;

public class IcebergPropertiesConstants {
  @VisibleForTesting
  public static String GRAVITINO_ICEBERG_CATALOG_BACKEND = IcebergConstants.CATALOG_BACKEND;

  public static final String ICEBERG_CATALOG_TYPE = FlinkCatalogFactory.ICEBERG_CATALOG_TYPE;

  public static final String ICEBERG_CATALOG_IMPL = CatalogProperties.CATALOG_IMPL;

  public static final String ICEBERG_CATALOG_BACKEND_JDBC = "jdbc";

  public static final String ICEBERG_JDBC_CATALOG_IMPL = "org.apache.iceberg.jdbc.JdbcCatalog";

  public static final String GRAVITINO_ICEBERG_CATALOG_WAREHOUSE = IcebergConstants.WAREHOUSE;

  public static final String ICEBERG_CATALOG_WAREHOUSE = CatalogProperties.WAREHOUSE_LOCATION;

  public static final String GRAVITINO_ICEBERG_CATALOG_URI = IcebergConstants.URI;

  public static final String ICEBERG_CATALOG_URI = CatalogProperties.URI;

  @VisibleForTesting
  public static String ICEBERG_CATALOG_BACKEND_HIVE = CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;

  public static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE = "hive";

  @VisibleForTesting
  public static final String ICEBERG_CATALOG_BACKEND_REST = CatalogUtil.ICEBERG_CATALOG_TYPE_REST;

  /** Gravitino Iceberg catalog property that selects how clients access table data. */
  public static final String GRAVITINO_ICEBERG_DATA_ACCESS = IcebergConstants.DATA_ACCESS;

  /** Iceberg catalog property that the data access mode is passed to the REST backend as. */
  public static final String ICEBERG_ACCESS_DELEGATION = IcebergConstants.ICEBERG_ACCESS_DELEGATION;

  /** Data access mode in which the REST backend vends per-table storage credentials. */
  public static final String ICEBERG_DATA_ACCESS_VENDED_CREDENTIALS = "vended-credentials";
}
