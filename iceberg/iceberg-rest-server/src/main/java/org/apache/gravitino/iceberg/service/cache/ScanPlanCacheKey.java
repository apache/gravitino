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

package org.apache.gravitino.iceberg.service.cache;

import com.google.common.base.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;

/** Cache key for Iceberg table scan plans. */
public class ScanPlanCacheKey {
  private final TableIdentifier tableIdentifier;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Expression filter; // Store the filter object instead of string
  private final String selectStr;
  private final String statsFieldsStr;
  private final boolean caseSensitive;
  private final boolean useSnapshotSchema;

  private ScanPlanCacheKey(
      TableIdentifier tableIdentifier,
      Long snapshotId,
      Long startSnapshotId,
      Long endSnapshotId,
      Expression filter,
      String select,
      String statsFields,
      boolean caseSensitive,
      boolean useSnapshotSchema) {
    this.tableIdentifier = tableIdentifier;
    this.snapshotId = snapshotId;
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
    this.filter = filter;
    this.selectStr = select;
    this.statsFieldsStr = statsFields;
    this.caseSensitive = caseSensitive;
    this.useSnapshotSchema = useSnapshotSchema;
  }

  /**
   * Creates a cache key from table identifier, table, and scan request.
   *
   * @param tableIdentifier the table identifier
   * @param table the Iceberg table
   * @param scanRequest the scan request containing filters and projections
   * @return a new cache key
   */
  public static ScanPlanCacheKey create(
      TableIdentifier tableIdentifier, Table table, PlanTableScanRequest scanRequest) {

    // Use current snapshot if not specified
    Long snapshotId = scanRequest.snapshotId();
    if (snapshotId == null && table.currentSnapshot() != null) {
      snapshotId = table.currentSnapshot().snapshotId();
    }

    // Include startSnapshotId and endSnapshotId in the key
    Long startSnapshotId = scanRequest.startSnapshotId();
    Long endSnapshotId = scanRequest.endSnapshotId();

    // Store the filter expression object directly.
    Expression filter = scanRequest.filter();

    // Sort select and statsFields to make key order-independent
    String selectStr = "";
    if (scanRequest.select() != null && !scanRequest.select().isEmpty()) {
      selectStr = scanRequest.select().stream().sorted().collect(Collectors.joining(","));
    }

    String statsFieldsStr = "";
    if (scanRequest.statsFields() != null && !scanRequest.statsFields().isEmpty()) {
      statsFieldsStr = scanRequest.statsFields().stream().sorted().collect(Collectors.joining(","));
    }

    return new ScanPlanCacheKey(
        tableIdentifier,
        snapshotId,
        startSnapshotId,
        endSnapshotId,
        filter,
        selectStr,
        statsFieldsStr,
        scanRequest.caseSensitive(),
        scanRequest.useSnapshotSchema());
  }

  public TableIdentifier getTableIdentifier() {
    return tableIdentifier;
  }

  public Long getSnapshotId() {
    return snapshotId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ScanPlanCacheKey)) {
      return false;
    }
    ScanPlanCacheKey that = (ScanPlanCacheKey) o;
    return caseSensitive == that.caseSensitive
        && useSnapshotSchema == that.useSnapshotSchema
        && Objects.equal(tableIdentifier, that.tableIdentifier)
        && Objects.equal(snapshotId, that.snapshotId)
        && Objects.equal(startSnapshotId, that.startSnapshotId)
        && Objects.equal(endSnapshotId, that.endSnapshotId)
        && Objects.equal(
            filter != null ? filter.toString() : null,
            that.filter != null ? that.filter.toString() : null)
        && Objects.equal(selectStr, that.selectStr)
        && Objects.equal(statsFieldsStr, that.statsFieldsStr);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(
        tableIdentifier,
        snapshotId,
        startSnapshotId,
        endSnapshotId,
        filter != null ? filter.toString() : null,
        selectStr,
        statsFieldsStr,
        caseSensitive,
        useSnapshotSchema);
  }

  @Override
  public String toString() {
    return String.format(
        "ScanPlanCacheKey{table=%s, snapshotId=%s, startSnapshotId=%s, endSnapshotId=%s, "
            + "filter=%s, select=%s, statsFields=%s, caseSensitive=%s, useSnapshotSchema=%s}",
        tableIdentifier,
        snapshotId,
        startSnapshotId,
        endSnapshotId,
        filter,
        selectStr.isEmpty() ? "null" : selectStr,
        statsFieldsStr.isEmpty() ? "null" : statsFieldsStr,
        caseSensitive,
        useSnapshotSchema);
  }
}
