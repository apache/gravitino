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

import com.google.common.annotations.VisibleForTesting;
import org.apache.gravitino.spark.connector.authorization.GravitinoAuthorizationSparkSessionExtensions;
import org.apache.gravitino.spark.connector.catalog.SparkCatalogKind;
import org.apache.gravitino.spark.connector.glue.GravitinoGlueCatalogSpark35;
import org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark35;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark35;
import org.apache.gravitino.spark.connector.jdbc.postgresql.GravitinoPostgreSqlCatalogSpark35;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.SparkPlugin;

/**
 * The entrypoint for Apache Gravitino Spark connector on Spark 3.5.
 *
 * <p>Every supported Spark version has a class of this name in this package, which is what users
 * name in {@code spark.plugins}. Each one binds the classes its own Spark version needs, as
 * compile-time class references, so a renamed or missing class fails the build rather than the
 * session, and a jar can only bind classes it actually contains.
 */
public class GravitinoSparkPlugin implements SparkPlugin {

  /**
   * The Paimon catalog is the one binding that cannot be a class reference: Paimon publishes no
   * {@code paimon-spark-3.5_2.13} artifact, so the Scala 2.13 build compiles the Paimon package out
   * and the class is absent from that jar. Resolved by presence below, and checked against the real
   * class by a test in that package, which the same build exclusion drops on 2.13.
   */
  @VisibleForTesting
  public static final String PAIMON_CATALOG =
      "org.apache.gravitino.spark.connector.paimon.GravitinoPaimonCatalogSpark35";

  @Override
  public DriverPlugin driverPlugin() {
    return new GravitinoDriverPlugin(bindings());
  }

  @Override
  public ExecutorPlugin executorPlugin() {
    return null;
  }

  private static SparkBindings bindings() {
    SparkBindings.Builder builder =
        SparkBindings.builder()
            .authorizationExtension(GravitinoAuthorizationSparkSessionExtensions.class)
            .catalog(SparkCatalogKind.HIVE, GravitinoHiveCatalogSpark35.class)
            .catalog(SparkCatalogKind.LAKEHOUSE_ICEBERG, GravitinoIcebergCatalogSpark35.class)
            .catalog(SparkCatalogKind.GLUE, GravitinoGlueCatalogSpark35.class)
            .catalog(SparkCatalogKind.JDBC, GravitinoJdbcCatalogSpark35.class)
            .catalog(SparkCatalogKind.JDBC_POSTGRESQL, GravitinoPostgreSqlCatalogSpark35.class);
    if (isPresent(PAIMON_CATALOG)) {
      builder.catalog(SparkCatalogKind.LAKEHOUSE_PAIMON, PAIMON_CATALOG);
    }
    return builder.build();
  }

  private static boolean isPresent(String className) {
    try {
      // initialize=false still loads and links the class, so its Paimon supertypes are resolved
      // here. A Spark 3.5 deployment that does not add the Paimon runtime, which is the documented
      // default, reaches this with the catalog class present but its supertypes missing, surfacing
      // as NoClassDefFoundError rather than ClassNotFoundException. Both mean the same thing to the
      // caller: this build cannot offer a Paimon catalog.
      Class.forName(className, false, GravitinoSparkPlugin.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }
}
