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

package org.apache.gravitino.spark.connector.authorization;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * A placeholder Spark table returned when the caller lacks the Gravitino privileges to load a
 * table.
 *
 * <p>The connector does not load the real Spark table for a denied table, because that load would
 * bypass the very authorization the caller is missing. Instead {@link #deny} records the table and
 * its required privileges in a per-thread collector and returns this placeholder so Spark's {@code
 * ResolveRelations} can finish resolving every relation in the query. {@link
 * RequiredPrivilegesCheck} then drains the collector once resolution completes and reports all
 * denied tables together, before analysis fails on anything else.
 *
 * <p>The metadata methods ({@link #name}, {@link #schema}, ...) return harmless placeholders so the
 * relation can be built during resolution, while the data-access methods ({@link #newScanBuilder},
 * {@link #newWriteBuilder}) fail closed: should the authorization check ever be bypassed, the table
 * still cannot be read or written.
 */
public class AuthorizationTable implements Table, SupportsRead, SupportsWrite {

  // Denied tables discovered while resolving the relations of a single query, keyed by the fully
  // qualified table identifier. Held per-thread because Spark analyzes one query per thread.
  private static final ThreadLocal<DeniedTables> DENIED_TABLES =
      ThreadLocal.withInitial(DeniedTables::new);

  private static final StructType EMPTY_SCHEMA = new StructType();
  private static final Transform[] EMPTY_PARTITIONING = new Transform[0];
  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(
          TableCapability.BATCH_READ,
          TableCapability.BATCH_WRITE,
          TableCapability.TRUNCATE,
          TableCapability.OVERWRITE_BY_FILTER,
          TableCapability.OVERWRITE_DYNAMIC);

  private final String name;
  private final ForbiddenException forbiddenException;

  private AuthorizationTable(String name, ForbiddenException forbiddenException) {
    this.name = name;
    this.forbiddenException = forbiddenException;
  }

  /**
   * Records a denied table for the current thread and returns a placeholder table to keep in the
   * analyzed plan.
   *
   * @param name the simple table name surfaced to Spark
   * @param tableIdentifier the fully qualified Gravitino table identifier
   * @param requiredPrivileges the privileges required to use the table
   * @param forbiddenException the original authorization failure
   * @return a placeholder table carrying the authorization failure
   */
  public static Table deny(
      String name,
      String tableIdentifier,
      Set<Privilege.Name> requiredPrivileges,
      ForbiddenException forbiddenException) {
    DENIED_TABLES.get().record(tableIdentifier, requiredPrivileges, forbiddenException);
    return new AuthorizationTable(name, forbiddenException);
  }

  /**
   * Returns an aggregated authorization failure for every denied table collected on the current
   * thread, then clears the collector. Returns {@link Optional#empty()} when no table was denied.
   *
   * @return the aggregated failure, or empty if there is none
   */
  public static Optional<ForbiddenException> drainFailure() {
    try {
      return DENIED_TABLES.get().failure();
    } finally {
      clear();
    }
  }

  /** Clears the denied tables collected on the current thread. */
  public static void clear() {
    DENIED_TABLES.remove();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public StructType schema() {
    return EMPTY_SCHEMA;
  }

  @Override
  public Transform[] partitioning() {
    return EMPTY_PARTITIONING;
  }

  @Override
  public Map<String, String> properties() {
    return Collections.emptyMap();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return CAPABILITIES;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    throw forbiddenException;
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    throw forbiddenException;
  }

  private static class DeniedTables {
    private final Map<String, Set<Privilege.Name>> tables = new TreeMap<>();
    private ForbiddenException firstFailure;

    private void record(
        String tableIdentifier,
        Set<Privilege.Name> requiredPrivileges,
        ForbiddenException forbiddenException) {
      tables
          .computeIfAbsent(tableIdentifier, ignored -> new TreeSet<>())
          .addAll(requiredPrivileges);
      if (firstFailure == null) {
        firstFailure = forbiddenException;
      }
    }

    private Optional<ForbiddenException> failure() {
      if (tables.isEmpty()) {
        return Optional.empty();
      }

      String requirements =
          tables.entrySet().stream()
              .map(
                  entry ->
                      entry.getKey()
                          + ": "
                          + entry.getValue().stream()
                              .map(Privilege.Name::name)
                              .collect(Collectors.joining(", ")))
              .collect(Collectors.joining("; "));
      return Optional.of(
          new ForbiddenException(
              firstFailure, "Missing required privileges for Spark tables: [%s]", requirements));
    }
  }
}
