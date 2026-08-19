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
import org.apache.gravitino.spark.connector.glue.GravitinoGlueCatalogSpark35;
import org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark35;
import org.apache.gravitino.spark.connector.iceberg.GravitinoIcebergCatalogSpark35;
import org.apache.gravitino.spark.connector.jdbc.GravitinoJdbcCatalogSpark35;
import org.apache.gravitino.spark.connector.jdbc.postgresql.GravitinoPostgreSqlCatalogSpark35;

/**
 * The catalogs the Spark 3.5 connector ships.
 *
 * <p>Every supported Spark version has a class of this name in this package, so the driver plugin
 * gets the right table from whichever connector jar is on the classpath, without reading the
 * running Spark version. The names are compile-time class references, so a renamed catalog fails
 * the build rather than the session, and a jar can only name catalogs it actually contains.
 */
public final class SparkCatalogs {

  /**
   * The Paimon catalog is the one entry that cannot be a compile-time reference: Paimon publishes
   * no {@code paimon-spark-3.5_2.13} artifact, so the Scala 2.13 build compiles the Paimon package
   * out and the class is absent from that jar. Named here rather than in shared code because the
   * name is version-specific, and resolved by presence below.
   */
  private static final String PAIMON_CATALOG =
      "org.apache.gravitino.spark.connector.paimon.GravitinoPaimonCatalogSpark35";

  private static final Map<SparkCatalogKind, String> CLASS_NAMES = buildClassNames();

  private SparkCatalogs() {}

  /**
   * Returns the Spark catalog class name for each kind of catalog this connector implements.
   *
   * @return catalog kind to Spark catalog class name
   */
  public static Map<SparkCatalogKind, String> classNames() {
    return CLASS_NAMES;
  }

  private static Map<SparkCatalogKind, String> buildClassNames() {
    ImmutableMap.Builder<SparkCatalogKind, String> builder =
        ImmutableMap.<SparkCatalogKind, String>builder()
            .put(SparkCatalogKind.HIVE, GravitinoHiveCatalogSpark35.class.getName())
            .put(SparkCatalogKind.LAKEHOUSE_ICEBERG, GravitinoIcebergCatalogSpark35.class.getName())
            .put(SparkCatalogKind.GLUE, GravitinoGlueCatalogSpark35.class.getName())
            .put(SparkCatalogKind.JDBC, GravitinoJdbcCatalogSpark35.class.getName())
            .put(
                SparkCatalogKind.JDBC_POSTGRESQL,
                GravitinoPostgreSqlCatalogSpark35.class.getName());
    if (isPresent(PAIMON_CATALOG)) {
      builder.put(SparkCatalogKind.LAKEHOUSE_PAIMON, PAIMON_CATALOG);
    }
    return builder.build();
  }

  private static boolean isPresent(String className) {
    try {
      // initialize=false still loads and links the class, so its Paimon supertypes are resolved
      // here. A Spark 3.5 deployment that does not add the Paimon runtime -- the documented default
      // -- reaches this with the catalog class present but its supertypes missing, which surfaces
      // as
      // NoClassDefFoundError rather than ClassNotFoundException. Both mean the same thing to the
      // caller: this build cannot offer a Paimon catalog.
      Class.forName(className, false, SparkCatalogs.class.getClassLoader());
      return true;
    } catch (ClassNotFoundException | LinkageError e) {
      return false;
    }
  }
}
