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
package org.apache.gravitino.catalog.hive;

public class HiveConstants {
  // Catalog properties
  public static final String METASTORE_URIS = "metastore.uris";
  public static final String CLIENT_POOL_SIZE = "client.pool-size";
  public static final String CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS =
      "client.pool-cache.eviction-interval-ms";
  public static final String IMPERSONATION_ENABLE = "impersonation-enable";
  public static final String KEY_TAB_URI = "kerberos.keytab-uri";
  public static final String PRINCIPAL = "kerberos.principal";
  public static final String CHECK_INTERVAL_SEC = "kerberos.check-interval-sec";
  public static final String FETCH_TIMEOUT_SEC = "kerberos.keytab-fetch-timeout-sec";
  public static final String LIST_ALL_TABLES = "list-all-tables";

  // table properties
  public static final String LOCATION = "location";
  public static final String COMMENT = "comment";
  public static final String NUM_FILES = "numFiles";
  public static final String TOTAL_SIZE = "totalSize";
  public static final String EXTERNAL = "EXTERNAL";
  public static final String FORMAT = "format";
  public static final String TABLE_TYPE = "table-type";
  public static final String INPUT_FORMAT = "input-format";
  public static final String OUTPUT_FORMAT = "output-format";
  public static final String SERDE_NAME = "serde-name";
  public static final String SERDE_LIB = "serde-lib";
  public static final String SERDE_PARAMETER_PREFIX = "serde.parameter.";
  public static final String TRANSIENT_LAST_DDL_TIME = "transient_lastDdlTime";
}
