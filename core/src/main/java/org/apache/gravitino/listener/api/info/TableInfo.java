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

package org.apache.gravitino.listener.api.info;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.DeveloperApi;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;

/**
 * TableInfo exposes table information for event listener, it's supposed to be read only. Most of
 * the fields are shallow copied internally not deep copies for performance.
 */
@DeveloperApi
public final class TableInfo {
  private final String name;
  private final Column[] columns;
  @Nullable private final String comment;
  private final Map<String, String> properties;
  private final Transform[] partitions;
  private final Distribution distribution;
  private final SortOrder[] sortOrders;
  private final Index[] indexes;
  @Nullable private final Audit auditInfo;

  /**
   * Constructs a TableInfo object from a Table instance.
   *
   * @param table The source Table instance.
   */
  public TableInfo(Table table) {
    this(
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

  /**
   * Constructs a TableInfo object with specified details.
   *
   * @param name Name of the table.
   * @param columns Array of columns in the table.
   * @param comment Optional comment about the table.
   * @param properties Map of table properties.
   * @param partitions Array of partition transforms.
   * @param distribution Table distribution configuration.
   * @param sortOrders Array of sort order configurations.
   * @param indexes Array of indexes on the table.
   * @param auditInfo Optional audit information.
   */
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
    this.properties = properties == null ? ImmutableMap.of() : ImmutableMap.copyOf(properties);
    this.partitions = partitions == null ? new Transform[0] : partitions.clone();
    this.distribution = distribution == null ? Distributions.NONE : distribution;
    this.sortOrders = sortOrders == null ? new SortOrder[0] : sortOrders.clone();
    this.indexes = indexes == null ? Indexes.EMPTY_INDEXES : indexes.clone();
    this.auditInfo = auditInfo;
  }

  /**
   * Returns the audit information for the table.
   *
   * @return Audit information, or {@code null} if not available.
   */
  @Nullable
  public Audit auditInfo() {
    return this.auditInfo;
  }

  /**
   * Returns the name of the table.
   *
   * @return Table name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the columns of the table.
   *
   * @return Array of table columns.
   */
  public Column[] columns() {
    return columns;
  }

  /**
   * Returns the partitioning transforms applied to the table.
   *
   * @return Array of partition transforms.
   */
  public Transform[] partitioning() {
    return partitions;
  }

  /**
   * Returns the sort order configurations for the table.
   *
   * @return Array of sort orders.
   */
  public SortOrder[] sortOrder() {
    return sortOrders;
  }

  /**
   * Returns the distribution configuration for the table.
   *
   * @return Distribution configuration.
   */
  public Distribution distribution() {
    return distribution;
  }

  /**
   * Returns the indexes applied to the table.
   *
   * @return Array of indexes.
   */
  public Index[] index() {
    return indexes;
  }

  /**
   * Returns the optional comment about the table.
   *
   * @return Table comment, or {@code null} if not provided.
   */
  @Nullable
  public String comment() {
    return comment;
  }

  /**
   * Returns the properties associated with the table.
   *
   * @return Map of table properties.
   */
  public Map<String, String> properties() {
    return properties;
  }
}
