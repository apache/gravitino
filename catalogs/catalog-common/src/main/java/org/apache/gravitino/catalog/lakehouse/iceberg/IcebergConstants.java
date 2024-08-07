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
package org.apache.gravitino.catalog.lakehouse.iceberg;

public class IcebergConstants {
  // Iceberg catalog properties constants
  public static final String CATALOG_BACKEND = "catalog-backend";

  public static final String GRAVITINO_JDBC_USER = "jdbc-user";
  public static final String ICEBERG_JDBC_USER = "jdbc.user";

  public static final String GRAVITINO_JDBC_PASSWORD = "jdbc-password";
  public static final String ICEBERG_JDBC_PASSWORD = "jdbc.password";
  public static final String ICEBERG_JDBC_INITIALIZE = "jdbc-initialize";

  public static final String GRAVITINO_JDBC_DRIVER = "jdbc-driver";
  public static final String WAREHOUSE = "warehouse";
  public static final String URI = "uri";
  public static final String CATALOG_BACKEND_NAME = "catalog-backend-name";

  // IO properties
  public static final String IO_IMPL = "io-impl";
  public static final String GRAVITINO_S3_ENDPOINT = "s3-endpoint";
  public static final String ICEBERG_S3_ENDPOINT = "s3.endpoint";
  public static final String GRAVITINO_S3_ACCESS_KEY_ID = "s3-access-key-id";
  public static final String ICEBERG_S3_ACCESS_KEY_ID = "s3.access-key-id";
  public static final String GRAVITINO_S3_SECRET_ACCESS_KEY = "s3-secret-access-key";
  public static final String ICEBERG_S3_SECRET_ACCESS_KEY = "s3.secret-access-key";
  public static final String GRAVITINO_S3_REGION = "s3-region";
  public static final String AWS_S3_REGION = "client.region";

  // Iceberg Table properties constants

  public static final String COMMENT = "comment";
  public static final String CREATOR = "creator";
  public static final String LOCATION = "location";
  public static final String CURRENT_SNAPSHOT_ID = "current-snapshot-id";
  public static final String CHERRY_PICK_SNAPSHOT_ID = "cherry-pick-snapshot-id";
  public static final String SORT_ORDER = "sort-order";
  public static final String IDENTIFIER_FIELDS = "identifier-fields";
  public static final String PROVIDER = "provider";
  public static final String FORMAT = "format";
  public static final String FORMAT_VERSION = "format-version";

  public static final String ICEBERG_METRICS_STORE = "metricsStore";
  public static final String ICEBERG_METRICS_STORE_RETAIN_DAYS = "metricsStoreRetainDays";
  public static final String ICEBERG_METRICS_QUEUE_CAPACITY = "metricsQueueCapacity";

  public static final String GRAVITINO_ICEBERG_REST_SERVICE_NAME = "iceberg-rest";
}
