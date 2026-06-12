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
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.types.StructType;

/** A Spark table used during analysis to retain denied Gravitino privilege information. */
public final class AuthorizationTable implements Table, SupportsWrite, SupportsRequiredPrivileges {

  private final Table delegate;
  private final String tableIdentifier;
  private final Set<Privilege.Name> requiredPrivileges;
  private final ForbiddenException forbiddenException;

  private AuthorizationTable(
      Table delegate,
      String tableIdentifier,
      Set<Privilege.Name> requiredPrivileges,
      ForbiddenException forbiddenException) {
    this.delegate = delegate;
    this.tableIdentifier = tableIdentifier;
    this.requiredPrivileges = ImmutableSet.copyOf(requiredPrivileges);
    this.forbiddenException = forbiddenException;
  }

  /**
   * Wraps a Spark table with the authorization failure that must be reported during analysis.
   *
   * @param delegate the underlying Spark table
   * @param tableIdentifier the fully qualified Gravitino table identifier
   * @param requiredPrivileges the privileges required to load the table
   * @param forbiddenException the original authorization failure
   * @return a table carrying the authorization failure
   */
  public static Table wrap(
      Table delegate,
      String tableIdentifier,
      Set<Privilege.Name> requiredPrivileges,
      ForbiddenException forbiddenException) {
    return new AuthorizationTable(
        delegate, tableIdentifier, requiredPrivileges, forbiddenException);
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public StructType schema() {
    return delegate.schema();
  }

  @Override
  public Transform[] partitioning() {
    return delegate.partitioning();
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return delegate.capabilities();
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return ((SupportsWrite) delegate).newWriteBuilder(info);
  }

  @Override
  public String tableIdentifier() {
    return tableIdentifier;
  }

  @Override
  public Set<Privilege.Name> requiredPrivileges() {
    return requiredPrivileges;
  }

  @Override
  public ForbiddenException forbiddenException() {
    return forbiddenException;
  }
}
