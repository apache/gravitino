/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.dto.rel;

import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.rel.indexes.IndexDTO;
import com.datastrato.gravitino.dto.rel.partitioning.Partitioning;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.indexes.Index;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.Map;

/** Represents a Table DTO (Data Transfer Object). */
public class TableDTO implements Table {

  @JsonProperty("name")
  private String name;

  @JsonProperty("comment")
  private String comment;

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
   * @param indexes Teh indexes of the table.
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
      IndexDTO[] indexes) {
    this.name = name;
    this.comment = comment;
    this.columns = columns;
    this.properties = properties;
    this.audit = audit;
    this.distribution = distribution;
    this.sortOrders = sortOrderDTOs;
    this.partitioning = partitioning;
    this.indexes = indexes;
  }

  /** @return The name of the table. */
  @Override
  public String name() {
    return name;
  }

  /** @return The columns of the table. */
  @Override
  public Column[] columns() {
    return columns;
  }

  /** @return The comment associated with the table. */
  @Override
  public String comment() {
    return comment;
  }

  /** @return The properties associated with the table. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** @return The audit information for the table. */
  @Override
  public AuditDTO auditInfo() {
    return audit;
  }

  /** @return The partitioning of the table. */
  @Override
  public Transform[] partitioning() {
    return partitioning;
  }

  /** @return The sort orders of the table. */
  @Override
  public SortOrder[] sortOrder() {
    return sortOrders;
  }

  /** @return The distribution of the table. */
  @Override
  public Distribution distribution() {
    return distribution;
  }

  /** @return The indexes of the table. */
  @Override
  public Index[] index() {
    return indexes;
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
    protected ColumnDTO[] columns;
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

    /** Default constructor. */
    public Builder() {}

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
     * @param columns The columns of the table.
     * @return The Builder instance.
     */
    public S withColumns(ColumnDTO[] columns) {
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
     * Builds a Table DTO based on the provided builder parameters.
     *
     * @return A new TableDTO instance.
     * @throws IllegalArgumentException If required fields name, columns and audit are not set.
     */
    public TableDTO build() {
      Preconditions.checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
      Preconditions.checkArgument(
          columns != null && columns.length > 0, "columns cannot be null or empty");
      Preconditions.checkArgument(audit != null, "audit cannot be null");

      return new TableDTO(
          name,
          comment,
          columns,
          properties,
          audit,
          Partitioning,
          distributionDTO,
          sortOrderDTOs,
          indexes);
    }
  }
}
