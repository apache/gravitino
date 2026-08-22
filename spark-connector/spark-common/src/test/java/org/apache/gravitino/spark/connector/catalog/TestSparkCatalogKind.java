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
package org.apache.gravitino.spark.connector.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests the provider to catalog kind mapping shared by every Spark version. */
public class TestSparkCatalogKind {

  @Test
  void testProvidersWithADedicatedCatalog() {
    Assertions.assertEquals(SparkCatalogKind.HIVE, SparkCatalogKind.fromProvider("hive"));
    Assertions.assertEquals(
        SparkCatalogKind.LAKEHOUSE_ICEBERG, SparkCatalogKind.fromProvider("lakehouse-iceberg"));
    Assertions.assertEquals(
        SparkCatalogKind.LAKEHOUSE_PAIMON, SparkCatalogKind.fromProvider("lakehouse-paimon"));
    Assertions.assertEquals(SparkCatalogKind.GLUE, SparkCatalogKind.fromProvider("glue"));
  }

  @Test
  void testEveryJdbcBackendSharesOneCatalogExceptPostgreSql() {
    Assertions.assertEquals(SparkCatalogKind.JDBC, SparkCatalogKind.fromProvider("jdbc-mysql"));
    Assertions.assertEquals(SparkCatalogKind.JDBC, SparkCatalogKind.fromProvider("jdbc-doris"));
    Assertions.assertEquals(SparkCatalogKind.JDBC, SparkCatalogKind.fromProvider("jdbc-starrocks"));
    Assertions.assertEquals(
        SparkCatalogKind.JDBC_POSTGRESQL, SparkCatalogKind.fromProvider("jdbc-postgresql"));
  }

  @Test
  void testProviderIsMatchedCaseInsensitively() {
    Assertions.assertEquals(SparkCatalogKind.HIVE, SparkCatalogKind.fromProvider("HIVE"));
    Assertions.assertEquals(
        SparkCatalogKind.JDBC_POSTGRESQL, SparkCatalogKind.fromProvider("JDBC-PostgreSQL"));
  }

  @Test
  void testUnknownProviderHasNoCatalog() {
    Assertions.assertNull(SparkCatalogKind.fromProvider("kafka"));
    Assertions.assertNull(SparkCatalogKind.fromProvider(""));
  }
}
