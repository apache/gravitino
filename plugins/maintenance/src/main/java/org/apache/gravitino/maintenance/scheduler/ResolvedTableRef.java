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
package org.apache.gravitino.maintenance.scheduler;

import java.util.Objects;

/** Catalog name and {@code schema.table} already known for one table id. */
public final class ResolvedTableRef {

  private final long tableId;
  private final String catalogName;
  private final String tableIdentifier;

  /**
   * @param tableId table entity id
   * @param catalogName catalog name
   * @param tableIdentifier {@code schema.table} Spark identifier
   */
  public ResolvedTableRef(long tableId, String catalogName, String tableIdentifier) {
    this.tableId = tableId;
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
   * @return catalog name
   */
  public String catalogName() {
    return catalogName;
  }

  /**
   * @return {@code schema.table} (or nested-namespace) Spark identifier
   */
  public String tableIdentifier() {
    return tableIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResolvedTableRef)) {
      return false;
    }
    ResolvedTableRef that = (ResolvedTableRef) o;
    return tableId == that.tableId
        && Objects.equals(catalogName, that.catalogName)
        && Objects.equals(tableIdentifier, that.tableIdentifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, catalogName, tableIdentifier);
  }
}
