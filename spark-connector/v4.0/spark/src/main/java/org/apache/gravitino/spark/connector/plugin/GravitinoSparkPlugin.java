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

package org.apache.gravitino.spark.connector.plugin;

import org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions;
import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.apache.gravitino.spark.connector.glue.GravitinoGlueCatalogSpark40;
import org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark40;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark40;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark40;
import org.apache.gravitino.spark.connector.jdbc.postgresql.GravitinoPostgreSqlCatalogSpark40;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/**
 * The entrypoint for Apache Gravitino Spark connector on Spark 4.0.
 *
 * <p>Every supported Spark version has a class of this name in this package, which is what users
 * name in {@code spark.plugins}. Each one binds the classes its own Spark version needs, as
 * compile-time class references, so a renamed or missing class fails the build rather than the
 * session, and a jar can only bind classes it actually contains.
 *
 * <p>No Paimon catalog is bound: Paimon publishes no {@code paimon-spark-4.x} artifact at the
 * Paimon version this repository pins, so the Spark 4 build has no Paimon catalog to name.
 */
public class GravitinoSparkPlugin implements SparkPlugin {

  @Override
  public DriverPlugin driverPlugin() {
    return new GravitinoDriverPlugin(bindings());
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return null;
  }

  private static SparkBindings bindings() {
    return SparkBindings.builder()
        .authorizationExtension(GravitinoAuthorizationSparkSessionExtensions.class)
        .catalog(SparkCatalogKind.HIVE, GravitinoHiveCatalogSpark40.class)
        .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, GravitinoIcebergCatalogSpark40.class)
        .catalog(SparkCatalogKind.GLUE, GravitinoGlueCatalogSpark40.class)
        .catalog(SparkCatalogKind.JDBC, GravitinoJdbcCatalogSpark40.class)
        .catalog(SparkCatalogKind.JDBC_POSTGRESQL, GravitinoPostgreSqlCatalogSpark40.class)
        .build();
  }
}
