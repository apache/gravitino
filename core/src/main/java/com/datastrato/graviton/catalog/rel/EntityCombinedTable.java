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
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Table class to represent a table metadata object that combines the metadata both from {@link
 * Table} and {@link TableEntity}.
 */
public final class EntityCombinedTable implements Table {

  private final Table table;

  private final TableEntity tableEntity;

  private final Map<String, String> properties;

  private EntityCombinedTable(
      Table table, TableEntity tableEntity, Map<String, String> properties) {
    this.table = table;
    this.tableEntity = tableEntity;
    this.properties = properties;
  }

  public static EntityCombinedTable withHiddenProperties(
      Table table, TableEntity tableEntity, Set<String> hiddenProperties) {
    Map<String, String> resultProperties =
        table.properties().entrySet().stream()
            .filter(e -> !hiddenProperties.contains(e.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return new EntityCombinedTable(table, tableEntity, resultProperties);
  }

  public static EntityCombinedTable withHiddenProperties(
      Table table, Set<String> hiddenProperties) {
    return withHiddenProperties(table, null, hiddenProperties);
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
    return properties;
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
