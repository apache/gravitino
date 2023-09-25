/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.rel;

import com.datastrato.graviton.Audit;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.TableEntity;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.transforms.Transform;
import java.util.Map;

/**
 * A Table class to represent a table metadata object that combines the metadata both from {@link
 * Table} and {@link TableEntity}.
 */
public final class EntityCombinedTable implements Table {

  private final Table table;

  private final TableEntity tableEntity;

  private EntityCombinedTable(Table table, TableEntity tableEntity) {
    this.table = table;
    this.tableEntity = tableEntity;
  }

  public static EntityCombinedTable of(Table table, TableEntity tableEntity) {
    return new EntityCombinedTable(table, tableEntity);
  }

  public static EntityCombinedTable of(Table table) {
    return of(table, null);
  }

  @Override
  public String name() {
    return table.name();
  }

  @Override
  public String comment() {
    return table.comment();
  }

  @Override
  public Column[] columns() {
    return table.columns();
  }

  @Override
  public Map<String, String> properties() {
    return table.properties();
  }

  @Override
  public Transform[] partitioning() {
    return table.partitioning();
  }

  @Override
  public SortOrder[] sortOrder() {
    return table.sortOrder();
  }

  @Override
  public Distribution distribution() {
    return table.distribution();
  }

  @Override
  public Audit auditInfo() {
    AuditInfo mergedAudit =
        new AuditInfo.Builder()
            .withCreator(table.auditInfo().creator())
            .withCreateTime(table.auditInfo().createTime())
            .withLastModifier(table.auditInfo().lastModifier())
            .withLastModifiedTime(table.auditInfo().lastModifiedTime())
            .build();

    return tableEntity == null
        ? table.auditInfo()
        : mergedAudit.merge(tableEntity.auditInfo(), true /* overwrite */);
  }
}
