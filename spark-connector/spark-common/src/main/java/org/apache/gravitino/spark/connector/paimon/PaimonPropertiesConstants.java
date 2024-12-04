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

import org.apache.gravitino.catalog.lakehouse.paimon.PaimonConstants;

public class PaimonPropertiesConstants {

  public static final String GRAVITINO_PAIMON_CATALOG_BACKEND = PaimonConstants.CATALOG_BACKEND;
  static final String PAIMON_CATALOG_METASTORE = PaimonConstants.METASTORE;

  public static final String GRAVITINO_PAIMON_CATALOG_WAREHOUSE = PaimonConstants.WAREHOUSE;
  static final String PAIMON_CATALOG_WAREHOUSE = PaimonConstants.WAREHOUSE;

  public static final String GRAVITINO_PAIMON_CATALOG_URI = PaimonConstants.URI;
  static final String PAIMON_CATALOG_URI = PaimonConstants.URI;
  public static final String GRAVITINO_PAIMON_CATALOG_JDBC_USER =
      PaimonConstants.GRAVITINO_JDBC_USER;
  static final String PAIMON_CATALOG_JDBC_USER = PaimonConstants.PAIMON_JDBC_USER;

  public static final String GRAVITINO_PAIMON_CATALOG_JDBC_PASSWORD =
      PaimonConstants.GRAVITINO_JDBC_PASSWORD;
  static final String PAIMON_CATALOG_JDBC_PASSWORD = PaimonConstants.PAIMON_JDBC_PASSWORD;

  public static final String GRAVITINO_PAIMON_CATALOG_JDBC_DRIVER =
      PaimonConstants.GRAVITINO_JDBC_DRIVER;

  public static final String PAIMON_CATALOG_BACKEND_HIVE = "hive";
  static final String GRAVITINO_PAIMON_CATALOG_BACKEND_HIVE = "hive";

  public static final String PAIMON_CATALOG_BACKEND_JDBC = "jdbc";
  static final String GRAVITINO_PAIMON_CATALOG_BACKEND_JDBC = "jdbc";

  public static final String PAIMON_CATALOG_BACKEND_FILESYSTEM = "filesystem";
  static final String GRAVITINO_PAIMON_CATALOG_BACKEND_FILESYSTEM = "filesystem";

  public static final String PAIMON_TABLE_LOCATION = "path";
}
