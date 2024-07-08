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
package com.datastrato.gravitino.iceberg.common;

public class IcebergConstants {
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

  public static final String ICEBERG_METRICS_STORE = "metricsStore";
  public static final String ICEBERG_METRICS_STORE_RETAIN_DAYS = "metricsStoreRetainDays";
  public static final String ICEBERG_METRICS_QUEUE_CAPACITY = "metricsQueueCapacity";

  public static final String GRAVITINO_ICEBERG_REST_SERVICE_NAME = "iceberg-rest";
}
