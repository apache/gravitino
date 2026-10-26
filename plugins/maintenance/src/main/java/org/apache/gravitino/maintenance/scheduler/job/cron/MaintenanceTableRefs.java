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
package org.apache.gravitino.maintenance.scheduler.job.cron;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.storage.relational.mapper.CatalogMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SchemaMetaMapper;
import org.apache.gravitino.storage.relational.mapper.TableMetaMapper;
import org.apache.gravitino.storage.relational.po.CatalogPO;
import org.apache.gravitino.storage.relational.po.SchemaPO;
import org.apache.gravitino.storage.relational.po.TablePO;
import org.apache.gravitino.storage.relational.utils.SessionUtils;

/** Resolves table entity ids to catalog + {@code schema.table} identifiers for Spark jobConf. */
public final class MaintenanceTableRefs {

  private MaintenanceTableRefs() {}

  /**
   * @param tableIds table entity ids (order preserved for known ids)
   * @return resolved refs; unknown / incomplete rows are omitted
   */
  public static List<ResolvedTableRef> resolve(List<Long> tableIds) {
    if (tableIds == null || tableIds.isEmpty()) {
      return List.of();
    }
    List<TablePO> tables =
        SessionUtils.getWithoutCommit(
            TableMetaMapper.class, mapper -> mapper.listTablePOsByTableIds(tableIds));
    if (tables == null || tables.isEmpty()) {
      return List.of();
    }
    Map<Long, TablePO> byId = new LinkedHashMap<>();
    for (TablePO table : tables) {
      if (table != null && table.getTableId() != null) {
        byId.put(table.getTableId(), table);
      }
    }

    Map<Long, String> catalogNames = new LinkedHashMap<>();
    Map<Long, String> schemaNames = new LinkedHashMap<>();
    List<ResolvedTableRef> resolved = new ArrayList<>();
    for (Long tableId : tableIds) {
      TablePO table = byId.get(tableId);
      if (table == null || table.getTableName() == null) {
        continue;
      }
      String catalogName = catalogName(table.getCatalogId(), catalogNames);
      String schemaName = schemaName(table.getSchemaId(), schemaNames);
      if (catalogName == null || schemaName == null) {
        continue;
      }
      resolved.add(
          new ResolvedTableRef(tableId, catalogName, schemaName + "." + table.getTableName()));
    }
    return resolved;
  }

  @Nullable
  private static String catalogName(Long catalogId, Map<Long, String> cache) {
    if (catalogId == null) {
      return null;
    }
    if (cache.containsKey(catalogId)) {
      return cache.get(catalogId);
    }
    CatalogPO catalog =
        SessionUtils.getWithoutCommit(
            CatalogMetaMapper.class, mapper -> mapper.selectCatalogMetaById(catalogId));
    String name = catalog == null ? null : catalog.getCatalogName();
    cache.put(catalogId, name);
    return name;
  }

  @Nullable
  private static String schemaName(Long schemaId, Map<Long, String> cache) {
    if (schemaId == null) {
      return null;
    }
    if (cache.containsKey(schemaId)) {
      return cache.get(schemaId);
    }
    SchemaPO schema =
        SessionUtils.getWithoutCommit(
            SchemaMetaMapper.class, mapper -> mapper.selectSchemaMetaById(schemaId));
    String name = schema == null ? null : schema.getSchemaName();
    cache.put(schemaId, name);
    return name;
  }

  /** One table ready for Spark expire-snapshots jobConf. */
  public static final class ResolvedTableRef {
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
}
