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
package org.apache.gravitino.dto.rel;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.indexes.IndexDTO;
import org.apache.gravitino.dto.rel.partitioning.Partitioning;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.SupportsPartitionSpecs;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/** Represents a Table DTO (Data Transfer Object). */
@EqualsAndHashCode
public class TableDTO implements Table, SupportsPartitionSpecs {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

  // Null only for the internal connector snapshot; responses always carry a column list.
  @Nullable
  @JsonProperty("columns")
  private ColumnDTO[] columns;

  @JsonProperty("properties")
  private Map<String, String> properties;

  @JsonProperty("audit")
  private AuditDTO audit;

  @JsonProperty("distribution")
  private DistributionDTO distribution;

  @JsonProperty("sortOrders")
  private SortOrderDTO[] sortOrders;

  @JsonProperty("partitioning")
  private Partitioning[] partitioning;

  @JsonProperty("indexes")
  private IndexDTO[] indexes;

  /**
   * Optional Iceberg-style default partition spec id. Present only when the source table supports
   * {@link SupportsPartitionSpecs}.
   */
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("defaultPartitionSpecId")
  private Integer defaultPartitionSpecId;

  /**
   * Optional map of partition specs keyed by {@code spec-id}. Present only when the source table
   * supports {@link SupportsPartitionSpecs}.
   */
  @Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty("partitionSpecs")
  private Map<Integer, Partitioning[]> partitionSpecs;

  private TableDTO() {}

  /**
   * Constructs a Table DTO.
   *
   * @param name The name of the table.
   * @param comment The comment associated with the table.
   * @param columns The columns of the table.
   * @param properties The properties associated with the table.
   * @param audit The audit information for the table.
   * @param partitioning The partitioning of the table.
   * @param indexes The indexes of the table.
   * @param defaultPartitionSpecId Optional default partition spec id.
   * @param partitionSpecs Optional historical partition specs keyed by spec id.
   */
  private TableDTO(
      String name,
      String comment,
      ColumnDTO[] columns,
      Map<String, String> properties,
      AuditDTO audit,
      Partitioning[] partitioning,
      DistributionDTO distribution,
      SortOrderDTO[] sortOrderDTOs,
      IndexDTO[] indexes,
      Integer defaultPartitionSpecId,
      Map<Integer, Partitioning[]> partitionSpecs) {
    this.name = name;
    this.comment = comment;
    this.columns = columns;
    this.properties = properties;
    this.audit = audit;
    this.distribution = distribution;
    this.sortOrders = sortOrderDTOs;
    this.partitioning = partitioning;
    this.indexes = indexes;
    this.defaultPartitionSpecId = defaultPartitionSpecId;
    this.partitionSpecs = partitionSpecs;
  }

  /**
   * @return The name of the table.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return The columns of the table. Null only for the internal connector snapshot of a table
   *     whose catalog reports no column list; such a table is rejected before it is returned to a
   *     client.
   */
  @Nullable
  @Override
  public Column[] columns() {
    return columns;
  }

  /**
   * @return The comment associated with the table.
   */
  @Override
  public String comment() {
    return comment;
  }

  /**
   * @return The properties associated with the table.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * @return The audit information for the table.
   */
  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /**
   * @return The partitioning of the table.
   */
  @Override
  public Transform[] partitioning() {
    return partitioning;
  }

  /**
   * @return The sort orders of the table.
   */
  @Override
  public SortOrder[] sortOrder() {
    return sortOrders;
  }

  /**
   * @return The distribution of the table.
   */
  @Override
  public Distribution distribution() {
    return distribution;
  }

  /**
   * @return The indexes of the table.
   */
  @Override
  public Index[] index() {
    return indexes;
  }

  /** {@inheritDoc} */
  @Override
  public SupportsPartitionSpecs supportsPartitionSpecs() {
    if (partitionSpecs == null || defaultPartitionSpecId == null) {
      throw new UnsupportedOperationException("Table does not support partition specs.");
    }
    validatePartitionSpecFields(defaultPartitionSpecId, partitionSpecs, partitioning);
    return this;
  }

  /**
   * Ensures optional partition-spec fields are consistent with each other and with {@code
   * partitioning}.
   *
   * @param defaultPartitionSpecId The default partition spec id, or null when specs are absent.
   * @param partitionSpecs Historical specs keyed by id, or null when absent.
   * @param partitioning The table's current partitioning transforms.
   */
  private static void validatePartitionSpecFields(
      Integer defaultPartitionSpecId,
      Map<Integer, Partitioning[]> partitionSpecs,
      Partitioning[] partitioning) {
    boolean hasDefaultSpecId = defaultPartitionSpecId != null;
    boolean hasPartitionSpecs = partitionSpecs != null;
    Preconditions.checkArgument(
        hasDefaultSpecId == hasPartitionSpecs,
        "defaultPartitionSpecId and partitionSpecs must both be set or both be null");
    if (!hasPartitionSpecs) {
      return;
    }
    Preconditions.checkArgument(
        partitionSpecs.containsKey(defaultPartitionSpecId),
        "defaultPartitionSpecId %s is missing from partitionSpecs %s",
        defaultPartitionSpecId,
        partitionSpecs.keySet());
    Partitioning[] defaultSpec = partitionSpecs.get(defaultPartitionSpecId);
    Partitioning[] current = partitioning == null ? Partitioning.EMPTY_PARTITIONING : partitioning;
    Partitioning[] expected = defaultSpec == null ? Partitioning.EMPTY_PARTITIONING : defaultSpec;
    Preconditions.checkArgument(
        Arrays.equals(current, expected),
        "partitioning must match partitionSpecs[%s]",
        defaultPartitionSpecId);
  }

  /** {@inheritDoc} */
  @Override
  public int defaultPartitionSpecId() {
    Preconditions.checkState(
        defaultPartitionSpecId != null, "Table does not carry a default partition spec id");
    return defaultPartitionSpecId;
  }

  /** {@inheritDoc} */
  @Override
  public Map<Integer, Transform[]> partitionSpecs() {
    Preconditions.checkState(partitionSpecs != null, "Table does not carry partition specs");
    Map<Integer, Transform[]> copied = new TreeMap<>();
    for (Map.Entry<Integer, Partitioning[]> entry : partitionSpecs.entrySet()) {
      Partitioning[] value = entry.getValue();
      copied.put(
          entry.getKey(),
          value == null ? new Transform[0] : Arrays.copyOf(value, value.length, Transform[].class));
    }
    return ImmutableMap.copyOf(copied);
  }

  /**
   * Creates a new Builder to build a Table DTO.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Builder class for constructing TableDTO instances.
   *
   * @param <S> The type of the builder subclass.
   */
  public static class Builder<S extends Builder> {
    /** The name of the table. */
    protected String name;
    /** The comment associated with the table. */
    protected String comment;
    /** The columns of the table. */
    @Nullable protected ColumnDTO[] columns;
    /** The properties associated with the table. */
    protected Map<String, String> properties;
    /** The audit information for the table. */
    protected AuditDTO audit;
    /** The distribution of the table. */
    protected SortOrderDTO[] sortOrderDTOs;
    /** The distribution of the table. */
    protected DistributionDTO distributionDTO;
    /** The partitioning of the table. */
    protected Partitioning[] Partitioning;
    /** The indexes of the table. */
    protected IndexDTO[] indexes;
    /** Optional default partition spec id. */
    @Nullable protected Integer defaultPartitionSpecId;
    /** Optional historical partition specs keyed by spec id. */
    @Nullable protected Map<Integer, Partitioning[]> partitionSpecs;

    /** Default constructor. */
    private Builder() {}

    /**
     * Sets the name of the table.
     *
     * @param name The name of the table.
     * @return The Builder instance.
     */
    public S withName(String name) {
      this.name = name;
      return (S) this;
    }

    /**
     * Sets the comment associated with the table.
     *
     * @param comment The comment associated with the table.
     * @return The Builder instance.
     */
    public S withComment(String comment) {
      this.comment = comment;
      return (S) this;
    }

    /**
     * Sets the columns of the table.
     *
     * @param columns The columns of the table. Null is only for the internal connector snapshot of
     *     a table whose catalog reports no column list; a DTO sent to clients always has columns.
     * @return The Builder instance.
     */
    public S withColumns(@Nullable ColumnDTO[] columns) {
      this.columns = columns;
      return (S) this;
    }

    /**
     * Sets the properties associated with the table.
     *
     * @param properties The properties associated with the table.
     * @return The Builder instance.
     */
    public S withProperties(Map<String, String> properties) {
      this.properties = properties;
      return (S) this;
    }

    /**
     * Sets the audit information for the table.
     *
     * @param audit The audit information for the table.
     * @return The Builder instance.
     */
    public S withAudit(AuditDTO audit) {
      this.audit = audit;
      return (S) this;
    }

    /**
     * Sets the distribution of the table.
     *
     * @param distributionDTO The distribution of the table.
     * @return The Builder instance.
     */
    public S withDistribution(DistributionDTO distributionDTO) {
      this.distributionDTO = distributionDTO;
      return (S) this;
    }

    /**
     * Sets the sort orders of the table.
     *
     * @param sortOrderDTOs The sort orders of the table.
     * @return The Builder instance.
     */
    public S withSortOrders(SortOrderDTO[] sortOrderDTOs) {
      this.sortOrderDTOs = sortOrderDTOs;
      return (S) this;
    }

    /**
     * Sets the partitioning of the table.
     *
     * @param Partitioning The partitioning of the table.
     * @return The Builder instance.
     */
    public S withPartitioning(Partitioning[] Partitioning) {
      this.Partitioning = Partitioning;
      return (S) this;
    }

    /**
     * Sets the indexes of the table.
     *
     * @param indexes The indexes of the table.
     * @return The Builder instance.
     */
    public S withIndex(IndexDTO[] indexes) {
      this.indexes = indexes;
      return (S) this;
    }

    /**
     * Sets the optional default partition spec id.
     *
     * @param defaultPartitionSpecId The default partition spec id.
     * @return The Builder instance.
     */
    public S withDefaultPartitionSpecId(@Nullable Integer defaultPartitionSpecId) {
      this.defaultPartitionSpecId = defaultPartitionSpecId;
      return (S) this;
    }

    /**
     * Sets the optional historical partition specs.
     *
     * @param partitionSpecs Partition specs keyed by spec id.
     * @return The Builder instance.
     */
    public S withPartitionSpecs(@Nullable Map<Integer, Partitioning[]> partitionSpecs) {
      this.partitionSpecs = partitionSpecs;
      return (S) this;
    }

    /**
     * Builds a Table DTO based on the provided builder parameters.
     *
     * @return A new TableDTO instance.
     * @throws IllegalArgumentException If required fields name and audit are not set.
     */
    public TableDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");
      validatePartitionSpecFields(defaultPartitionSpecId, partitionSpecs, Partitioning);

      return new TableDTO(
          name,
          comment,
          columns,
          properties,
          audit,
          Partitioning,
          distributionDTO,
          sortOrderDTOs,
          indexes,
          defaultPartitionSpecId,
          partitionSpecs);
    }

    /**
     * Creates a new instance of {@link Builder}.
     *
     * @return The new instance.
     */
    public static Builder builder() {
      return new Builder();
    }
  }
}
