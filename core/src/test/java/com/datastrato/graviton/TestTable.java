/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton;

import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.meta.rel.BaseTable;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.SortOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
public class TestTable extends BaseTable {

  protected Distribution distribution;

  protected SortOrder[] sortOrders;

  public static class Builder extends BaseTable.BaseTableBuilder<Builder, TestTable> {
    private Distribution distribution;

    private SortOrder[] sortOrders;

    public Builder withDistribution(Distribution distribution) {
      this.distribution = distribution;
      return this;
    }

    public Builder withSortOrders(SortOrder[] sortOrders) {
      this.sortOrders = sortOrders;
      return this;
    }

    @Override
    protected TestTable internalBuild() {
      TestTable table = new TestTable();
      table.id = id;
      table.name = name;
      table.comment = comment;
      table.namespace = namespace;
      table.properties = properties;
      table.columns = columns;
      table.auditInfo = auditInfo;
      table.distribution = distribution;
      table.sortOrders = sortOrders;
      table.partitions = partitions;
      return table;
    }
  }

  public TestTable(
      String name,
      Namespace namespace,
      String comment,
      Map<String, String> properties,
      AuditInfo auditInfo,
      Column[] columns,
      Distribution distribution,
      SortOrder[] sortOrders) {
    this.name = name;
    this.namespace = namespace;
    this.comment = comment;
    this.properties = properties;
    this.auditInfo = auditInfo;
    this.columns = columns;
    this.sortOrders = sortOrders;
    this.distribution = distribution;

    validate();
  }

  // For Jackson Deserialization only.
  public TestTable() {}

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(NAME, name);
    fields.put(COMMENT, comment);
    fields.put(PROPERTIES, properties);
    fields.put(AUDIT_INFO, auditInfo);

    return Collections.unmodifiableMap(fields);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Namespace namespace() {
    return namespace;
  }

  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public EntityType type() {
    return EntityType.TABLE;
  }
}
