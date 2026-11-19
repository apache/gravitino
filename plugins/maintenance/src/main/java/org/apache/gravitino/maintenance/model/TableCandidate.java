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
package org.apache.gravitino.maintenance.model;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.meta.PolicyEntity;

/**
 * One table plus the maintenance policies to schedule for it. Used by both cron and commit {@code
 * MaintenanceJobSpec} builders; each table may carry a different policy list.
 */
public final class TableCandidate {

  private final long tableId;
  private final String catalogName;
  private final String tableIdentifier;
  private final List<PolicyEntity> policies;
  @Nullable private final Integer defaultPartitionSpecId;

  /**
   * @param tableId table entity id
   * @param catalogName catalog name
   * @param tableIdentifier {@code schema.table} Spark identifier
   * @param policies policies for this table (order preserved; must not be empty)
   */
  public TableCandidate(
      long tableId, String catalogName, String tableIdentifier, List<PolicyEntity> policies) {
    this(tableId, catalogName, tableIdentifier, policies, null);
  }

  /**
   * @param tableId table entity id
   * @param catalogName catalog name
   * @param tableIdentifier {@code schema.table} Spark identifier
   * @param policies policies for this table (order preserved; must not be empty)
   * @param defaultPartitionSpecId Iceberg {@code default-spec-id} at schedule time; used to pin
   *     rewrite-manifests when the policy omits {@code spec_id}
   */
  public TableCandidate(
      long tableId,
      String catalogName,
      String tableIdentifier,
      List<PolicyEntity> policies,
      @Nullable Integer defaultPartitionSpecId) {
    Preconditions.checkArgument(tableId > 0, "tableId must be positive");
    Preconditions.checkArgument(
        catalogName != null && !catalogName.isBlank(), "catalogName must not be blank");
    Preconditions.checkArgument(
        tableIdentifier != null && !tableIdentifier.isBlank(), "tableIdentifier must not be blank");
    Preconditions.checkArgument(
        policies != null && !policies.isEmpty(), "policies must not be empty");
    Preconditions.checkArgument(
        defaultPartitionSpecId == null || defaultPartitionSpecId >= 0,
        "defaultPartitionSpecId must be >= 0");
    this.tableId = tableId;
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.policies = ImmutableList.copyOf(policies);
    this.defaultPartitionSpecId = defaultPartitionSpecId;
  }

  /**
   * @return table entity id
   */
  public long tableId() {
    return tableId;
  }

  /**
   * @return catalog name
   */
  public String catalogName() {
    return catalogName;
  }

  /**
   * @return {@code schema.table} Spark identifier
   */
  public String tableIdentifier() {
    return tableIdentifier;
  }

  /**
   * @return policies for this table in schedule order
   */
  public List<PolicyEntity> policies() {
    return policies;
  }

  /**
   * @return Iceberg {@code default-spec-id} captured at schedule time, or {@code null} when unknown
   */
  @Nullable
  public Integer defaultPartitionSpecId() {
    return defaultPartitionSpecId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TableCandidate)) {
      return false;
    }
    TableCandidate that = (TableCandidate) o;
    return tableId == that.tableId
        && catalogName.equals(that.catalogName)
        && tableIdentifier.equals(that.tableIdentifier)
        && policies.equals(that.policies)
        && Objects.equals(defaultPartitionSpecId, that.defaultPartitionSpecId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, catalogName, tableIdentifier, policies, defaultPartitionSpecId);
  }

  @Override
  public String toString() {
    return "TableCandidate{tableId="
        + tableId
        + ", catalogName="
        + catalogName
        + ", tableIdentifier="
        + tableIdentifier
        + ", defaultPartitionSpecId="
        + defaultPartitionSpecId
        + ", policyIds="
        + policies.stream().map(PolicyEntity::id).toList()
        + '}';
  }
}
