/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.rel;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.NoSuchPartitionException;
import com.datastrato.gravitino.exceptions.PartitionAlreadyExistsException;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.SupportsPartitions;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.partitions.Partition;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Table class to represent a table metadata object that combines the metadata both from {@link
 * Table} and {@link TableEntity}.
 */
public final class EntityCombinedTable implements SupportsPartitions {

  private final Table table;

  private final TableEntity tableEntity;

  // Sets of properties that should be hidden from the user.
  private Set<String> hiddenProperties;

  private EntityCombinedTable(Table table, TableEntity tableEntity) {
    this.table = table;
    this.tableEntity = tableEntity;
  }

  public static EntityCombinedTable of(Table table, TableEntity tableEntity) {
    return new EntityCombinedTable(table, tableEntity);
  }

  public static EntityCombinedTable of(Table table) {
    return new EntityCombinedTable(table, null);
  }

  public EntityCombinedTable withHiddenPropertiesSet(Set<String> hiddenProperties) {
    this.hiddenProperties = hiddenProperties;
    return this;
  }

  @Override
  public Namespace namespace() {
    return tableEntity.namespace();
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
    return table.properties().entrySet().stream()
        .filter(p -> !hiddenProperties.contains(p.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return table.supportPartitions();
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

  @Override
  public String[] listPartitionNames() {
    return supportPartitions().listPartitionNames();
  }

  @Override
  public Partition[] listPartitions() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String partitionName) throws NoSuchPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition addPartition(String partitionName, Map<String, String> properties)
      throws PartitionAlreadyExistsException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String partitionName) {
    throw new UnsupportedOperationException();
  }
}
