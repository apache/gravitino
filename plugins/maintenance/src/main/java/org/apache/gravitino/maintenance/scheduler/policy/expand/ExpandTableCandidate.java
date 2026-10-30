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
package org.apache.gravitino.maintenance.scheduler.policy.expand;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.gravitino.MetadataObject;

/**
 * One enqueueable table after nearest-policy and occupancy gating, with the metadata level where
 * the winning policy was selected and the catalog / table names already known at expand time.
 */
public final class ExpandTableCandidate {

  private final long tableId;
  private final MetadataObject.Type winnerLevel;
  private final String catalogName;
  private final String tableIdentifier;

  /**
   * @param tableId table entity id
   * @param winnerLevel {@link MetadataObject.Type#TABLE}, {@link MetadataObject.Type#SCHEMA}, or
   *     {@link MetadataObject.Type#CATALOG}
   * @param catalogName catalog name from the table metadata object
   * @param tableIdentifier {@code schema.table} Spark identifier
   */
  public ExpandTableCandidate(
      long tableId, MetadataObject.Type winnerLevel, String catalogName, String tableIdentifier) {
    Preconditions.checkArgument(tableId > 0, "tableId must be positive");
    Preconditions.checkNotNull(winnerLevel, "winnerLevel must not be null");
    Preconditions.checkArgument(
        catalogName != null && !catalogName.isBlank(), "catalogName must not be blank");
    Preconditions.checkArgument(
        tableIdentifier != null && !tableIdentifier.isBlank(), "tableIdentifier must not be blank");
    this.tableId = tableId;
    this.winnerLevel = winnerLevel;
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
  }

  /**
   * @return table entity id
   */
  public long tableId() {
    return tableId;
  }

  /**
   * @return level where the nearest winning policy was attached
   */
  public MetadataObject.Type winnerLevel() {
    return winnerLevel;
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
   * @return true when snapshot-expiry may pack this table into a {@code batch:...} cron-job
   */
  public boolean batchableForSnapshotExpiry() {
    return winnerLevel == MetadataObject.Type.SCHEMA || winnerLevel == MetadataObject.Type.CATALOG;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExpandTableCandidate)) {
      return false;
    }
    ExpandTableCandidate that = (ExpandTableCandidate) o;
    return tableId == that.tableId
        && winnerLevel == that.winnerLevel
        && catalogName.equals(that.catalogName)
        && tableIdentifier.equals(that.tableIdentifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, winnerLevel, catalogName, tableIdentifier);
  }

  @Override
  public String toString() {
    return "ExpandTableCandidate{tableId="
        + tableId
        + ", winnerLevel="
        + winnerLevel
        + ", catalogName="
        + catalogName
        + ", tableIdentifier="
        + tableIdentifier
        + '}';
  }
}
