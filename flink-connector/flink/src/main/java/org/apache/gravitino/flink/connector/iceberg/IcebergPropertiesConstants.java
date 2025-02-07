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

  public static final String GRAVITINO_ICEBERG_CATALOG_WAREHOUSE = IcebergConstants.WAREHOUSE;

  public static final String ICEBERG_CATALOG_WAREHOUSE = CatalogProperties.WAREHOUSE_LOCATION;

  public static final String GRAVITINO_ICEBERG_CATALOG_URI = IcebergConstants.URI;

  public static final String ICEBERG_CATALOG_URI = CatalogProperties.URI;

  @VisibleForTesting
  public static String ICEBERG_CATALOG_BACKEND_HIVE = CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE;

  public static final String GRAVITINO_ICEBERG_CATALOG_BACKEND_HIVE = "hive";

  @VisibleForTesting
  public static final String ICEBERG_CATALOG_BACKEND_REST = CatalogUtil.ICEBERG_CATALOG_TYPE_REST;
}
