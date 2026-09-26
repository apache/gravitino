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
package org.apache.gravitino.spark.connector.jdbc.doris;

/** Constants for the Spark 3.5 Apache Doris adapter. */
final class DorisConnectorConstants35 {

  static final String GRAVITINO_FE_NODES = "doris-fenodes";
  static final String GRAVITINO_QUERY_PORT = "doris-query-port";
  static final String GRAVITINO_WRITE_MODE = "doris-write-mode";
  static final String GRAVITINO_WRITE_OVERWRITE_MODE = "doris-write-overwrite-mode";
  static final String DORIS_FE_NODES = "doris.fenodes";
  static final String DORIS_QUERY_PORT = "doris.query.port";
  static final String DORIS_USER = "doris.user";
  static final String DORIS_PASSWORD = "doris.password";
  static final String WRITE_DISABLED = "disabled";
  static final String WRITE_BATCH = "batch";
  static final String WRITE_OVERWRITE_REJECT = "reject";
  static final String WRITE_OVERWRITE_TRUNCATE = "truncate";
  static final String DORIS_SINK_MODE = "doris.sink.mode";
  static final String DORIS_SINK_AUTO_REDIRECT = "doris.sink.auto-redirect";
  static final String DORIS_SINK_ENABLE_2PC = "doris.sink.enable-2pc";
  static final String DORIS_SINK_STRICT_MODE = "doris.sink.properties.strict_mode";
  static final String DORIS_MAX_FILTER_RATIO = "doris.max.filter.ratio";
  static final String DORIS_WRITE_SCHEMALESS = "doris.write.schemaless";
  static final String JDBC_URL = "jdbc-url";
  static final String JDBC_DRIVER = "jdbc-driver";
  static final String JDBC_PARTITION_COLUMN = "doris-jdbc-partition-column";
  static final String JDBC_LOWER_BOUND = "doris-jdbc-lower-bound";
  static final String JDBC_UPPER_BOUND = "doris-jdbc-upper-bound";
  static final String JDBC_NUM_PARTITIONS = "doris-jdbc-num-partitions";
  static final String JDBC_FETCH_SIZE = "doris-jdbc-fetch-size";

  private DorisConnectorConstants35() {}
}
