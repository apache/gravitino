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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.spark.connector.glue.GravitinoGlueCatalogSpark40;
import org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark40;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark40;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark40;
import org.apache.gravitino.spark.connector.jdbc.postgresql.GravitinoPostgreSqlCatalogSpark40;

/**
 * The catalogs the Spark 4.0 connector ships.
 *
 * <p>Every supported Spark version has a class of this name in this package, so the driver plugin
 * gets the right table from whichever connector jar is on the classpath without reading the running
 * Spark version. The names are compile-time class references, so a renamed catalog fails the build,
 * and a jar can only name catalogs it actually contains.
 *
 * <p>Paimon is absent here because Paimon publishes no {@code paimon-spark-4.x} artifact at the
 * Paimon version this repository pins, so the Spark 4 build has no Paimon catalog to name.
 */
public final class SparkCatalogs {

  private static final Map<SparkCatalogKind, String> CLASS_NAMES =
      ImmutableMap.<SparkCatalogKind, String>builder()
          .put(SparkCatalogKind.HIVE, GravitinoHiveCatalogSpark40.class.getName())
          .put(SparkCatalogKind.LAKEHOUSE_ICEBERG, GravitinoIcebergCatalogSpark40.class.getName())
          .put(SparkCatalogKind.GLUE, GravitinoGlueCatalogSpark40.class.getName())
          .put(SparkCatalogKind.JDBC, GravitinoJdbcCatalogSpark40.class.getName())
          .put(SparkCatalogKind.JDBC_POSTGRESQL, GravitinoPostgreSqlCatalogSpark40.class.getName())
          .build();

  private SparkCatalogs() {}

  /**
   * Returns the Spark catalog class name for each kind of catalog this connector implements.
   *
   * @return catalog kind to Spark catalog class name
   */
  public static Map<SparkCatalogKind, String> classNames() {
    return CLASS_NAMES;
  }
}
