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
package org.apache.gravitino.spark.connector.version;

import org.apache.gravitino.spark.connector.glue.GravitinoGlueCatalogSpark40;
import org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark40;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark40;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark40;
import org.apache.gravitino.spark.connector.jdbc.postgresql.GravitinoPostgreSqlCatalogSpark40;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies that the adaptor resolves the Spark 4.0 catalogs when running on Spark 4.0. */
public class TestCatalogNameAdaptor {

  @Test
  void testSpark40() {
    Assertions.assertEquals(
        GravitinoHiveCatalogSpark40.class.getName(), CatalogNameAdaptor.getCatalogName("hive"));
    Assertions.assertEquals(
        GravitinoIcebergCatalogSpark40.class.getName(),
        CatalogNameAdaptor.getCatalogName("lakehouse-iceberg"));
    Assertions.assertEquals(
        GravitinoGlueCatalogSpark40.class.getName(), CatalogNameAdaptor.getCatalogName("glue"));
    Assertions.assertEquals(
        GravitinoJdbcCatalogSpark40.class.getName(), CatalogNameAdaptor.getCatalogName("jdbc"));
    Assertions.assertEquals(
        GravitinoPostgreSqlCatalogSpark40.class.getName(),
        CatalogNameAdaptor.getCatalogName("jdbc-postgresql"));
  }

  @Test
  void testPaimonIsNotAvailableOnSpark40() {
    // Paimon publishes no paimon-spark-4.x artifact at the Paimon version this repository pins, so
    // the Paimon classes are absent from the Spark 4 builds and the adaptor has no mapping.
    Assertions.assertNull(CatalogNameAdaptor.getCatalogName("lakehouse-paimon"));
  }
}
