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
 * bypass the very authorization the caller is missing. Instead {@link #deny} returns this
 * placeholder - carrying the table identifier, the required privileges, and the original failure -
 * so Spark's {@code ResolveRelations} can finish resolving every relation in the query. {@link
 * RequiredPrivilegesCheck} then walks the analyzed plan, finds every placeholder, and reports all
 * denied tables together, before analysis fails on anything else.
 *
 * <p>Because the failure is carried in the plan itself rather than in side state, an analysis that
 * aborts before the post-hoc rule runs (for example, a catalog load that throws on a second table)
 * leaves nothing behind that can fail a later unrelated query on the same thread.
 *
 * <p>The metadata methods ({@link #name}, {@link #schema}, ...) return harmless placeholders so the
 * relation can be built during resolution, while the data-access methods ({@link #newScanBuilder},
 * {@link #newWriteBuilder}) fail closed: should the authorization check ever be bypassed, the table
 * still cannot be read or written.
 */
public class AuthorizationTable implements Table, SupportsRead, SupportsWrite {

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
  private final String tableIdentifier;
  private final Set<Privilege.Name> requiredPrivileges;
  private final ForbiddenException forbiddenException;

  private AuthorizationTable(
      String name,
      String tableIdentifier,
      Set<Privilege.Name> requiredPrivileges,
      ForbiddenException forbiddenException) {
    this.name = name;
    this.tableIdentifier = tableIdentifier;
    this.requiredPrivileges = requiredPrivileges;
    this.forbiddenException = forbiddenException;
  }

  /**
   * Returns a placeholder table carrying the authorization failure, to keep in the analyzed plan.
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
    return new AuthorizationTable(name, tableIdentifier, requiredPrivileges, forbiddenException);
  }

  /**
   * Aggregates the authorization failures carried by the given denied placeholders into one message
   * listing every table and its required privileges, in table-identifier order.
   *
   * @param deniedTables the denied placeholders found in a plan
   * @return the aggregated failure, or empty when nothing was denied
   */
  public static Optional<ForbiddenException> aggregateFailure(
      Iterable<AuthorizationTable> deniedTables) {
    Map<String, Set<Privilege.Name>> tables = new TreeMap<>();
    ForbiddenException firstFailure = null;
    for (AuthorizationTable denied : deniedTables) {
      tables
          .computeIfAbsent(denied.tableIdentifier, ignored -> new TreeSet<>())
          .addAll(denied.requiredPrivileges);
      if (firstFailure == null) {
        firstFailure = denied.forbiddenException;
      }
    }
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

  /** Returns the fully qualified Gravitino table identifier that was denied. */
  public String tableIdentifier() {
    return tableIdentifier;
  }

  /** Returns the privileges whose absence denied the table. */
  public Set<Privilege.Name> requiredPrivileges() {
    return requiredPrivileges;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  // Table.schema() is deprecated from Spark 3.4 in favor of columns(); this table carries no
  // columns, so the deprecated form is the simplest way to report an empty schema on every
  // supported version.
  @SuppressWarnings("deprecation")
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
}
