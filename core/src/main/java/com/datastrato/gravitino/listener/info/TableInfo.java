/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.info;

import com.datastrato.gravitino.Audit;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * TableInfo exposes table interfaces for event listener, it's supposed to be read only. Most of the
 * fields are shallow copied internally not deep copies for performance.
 */
public class TableInfo implements Table {
  private String name;
  private Column[] columns;
  @Nullable private String comment;
  private Map<String, String> properties;
  private Transform[] partitions;
  private Distribution distribution;
  private SortOrder[] sortOrders;
  private Index[] indexes;
  @Nullable private Audit auditInfo;

  public TableInfo(Table table) {
    new TableInfo(
        table.name(),
        table.columns(),
        table.comment(),
        table.properties(),
        table.partitioning(),
        table.distribution(),
        table.sortOrder(),
        table.index(),
        table.auditInfo());
  }

  public TableInfo(
      String name,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes,
      Audit auditInfo) {
    this.name = name;
    this.columns = columns.clone();
    this.comment = comment;
    if (properties == null) {
      this.properties = ImmutableMap.of();
    } else {
      this.properties = ImmutableMap.<String, String>builder().putAll(properties).build();
    }
    if (partitions == null) {
      this.partitions = new Transform[0];
    } else {
      this.partitions = partitions.clone();
    }
    if (distribution == null) {
      this.distribution = Distributions.NONE;
    } else {
      this.distribution = distribution;
    }
    if (sortOrders == null) {
      this.sortOrders = new SortOrder[0];
    } else {
      this.sortOrders = sortOrders.clone();
    }
    if (indexes == null) {
      this.indexes = Indexes.EMPTY_INDEXES;
    } else {
      this.indexes = indexes.clone();
    }
    this.auditInfo = auditInfo;
  }

  /** Audit information is null when tableInfo is generated from create table request. */
  @Override
  @Nullable
  public Audit auditInfo() {
    return this.auditInfo;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Column[] columns() {
    return columns;
  }

  @Override
  public Transform[] partitioning() {
    return partitions;
  }

  @Override
  public SortOrder[] sortOrder() {
    return sortOrders;
  }

  @Override
  public Distribution distribution() {
    return distribution;
  }

  @Override
  public Index[] index() {
    return indexes;
  }

  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }
}
