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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * The kinds of Spark catalog this connector implements, one per group of Gravitino catalog
 * providers that share an implementation.
 *
 * <p>Which kind a provider needs is the same on every Spark version, so that mapping lives here.
 * Which class implements a kind differs per version, so each version module binds that in its own
 * {@code GravitinoSparkPlugin}.
 */
public enum SparkCatalogKind {
  /** Hive catalogs, provider {@code hive}. */
  HIVE,
  /** Iceberg catalogs, provider {@code lakehouse-iceberg}. */
  LAKEHOUSE_ICEBERG,
  /** Paimon catalogs, provider {@code lakehouse-paimon}. */
  LAKEHOUSE_PAIMON,
  /** AWS Glue catalogs, provider {@code glue}. */
  GLUE,
  /** Every {@code jdbc-*} catalog except PostgreSQL, which has its own kind. */
  JDBC,
  /** PostgreSQL catalogs, provider {@code jdbc-postgresql}. */
  JDBC_POSTGRESQL;

  private static final String JDBC_PROVIDER_PREFIX = "jdbc";
  private static final String POSTGRESQL_PROVIDER_PREFIX = "jdbc-postgresql";

  private static final Map<String, SparkCatalogKind> KINDS_BY_PROVIDER =
      ImmutableMap.of(
          "hive",
          HIVE,
          "lakehouse-iceberg",
          LAKEHOUSE_ICEBERG,
          "lakehouse-paimon",
          LAKEHOUSE_PAIMON,
          "glue",
          GLUE);

  /**
   * Returns the kind of Spark catalog a Gravitino catalog provider needs.
   *
   * @param provider a Gravitino catalog provider, such as {@code hive} or {@code jdbc-mysql}
   * @return the matching kind, or null when this connector has no catalog for the provider
   * @throws NullPointerException if the provider is null. Callers hold a provider the server set,
   *     and a catalog with none is a server-side problem worth its own message rather than a null
   *     that quietly maps to no kind.
   */
  @Nullable
  public static SparkCatalogKind fromProvider(String provider) {
    Objects.requireNonNull(provider, "Catalog provider must not be null");
    String normalized = provider.toLowerCase(Locale.ROOT);
    // All JDBC backends share one Spark catalog, apart from PostgreSQL, whose type and property
    // conversions differ.
    if (normalized.startsWith(JDBC_PROVIDER_PREFIX)) {
      return normalized.startsWith(POSTGRESQL_PROVIDER_PREFIX) ? JDBC_POSTGRESQL : JDBC;
    }
    return KINDS_BY_PROVIDER.get(normalized);
  }
}
