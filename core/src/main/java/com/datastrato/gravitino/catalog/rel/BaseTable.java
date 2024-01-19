/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.rel;

import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.ToString;

/** An abstract class representing a base table in a relational database. */
@ToString
public abstract class BaseTable implements Table {

  protected Namespace namespace;

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  protected Column[] columns;

  @Nullable protected Transform[] partitioning;

  @Nullable protected SortOrder[] sortOrders;

  @Nullable protected Distribution distribution;

  public Namespace namespace() {
    return namespace;
  }

  /** Returns the audit details of the table. */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /** Returns the name of the table. */
  @Override
  public String name() {
    return name;
  }

  /** Returns an array of columns that make up the table. */
  @Override
  public Column[] columns() {
    return columns;
  }

  /** Returns the comment or description for the table. */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /** Returns the associated properties of the table. */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /** Returns the partitioning strategies of the table. */
  @Override
  public Transform[] partitioning() {
    return partitioning;
  }

  /** Return the array of {@link SortOrder} of the table. */
  @Override
  public SortOrder[] sortOrder() {
    return sortOrders;
  }

  /**
   * Returns the distribution strategy of the table.
   *
   * @return The distribution strategy of the table.
   */
  @Override
  public Distribution distribution() {
    return distribution;
  }

  /**
   * Builder interface for creating instances of {@link BaseTable}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the table being built.
   */
  interface Builder<SELF extends Builder<SELF, T>, T extends BaseTable> {

    SELF withNamespace(Namespace namespace);

    SELF withName(String name);

    SELF withColumns(Column[] columns);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    SELF withPartitioning(Transform[] partitioning);

    SELF withSortOrders(SortOrder[] sortOrders);

    SELF withDistribution(Distribution distribution);

    T build();
  }

  /**
   * An abstract class implementing the builder interface for {@link BaseTable}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the table being built.
   */
  public abstract static class BaseTableBuilder<SELF extends Builder<SELF, T>, T extends BaseTable>
      implements Builder<SELF, T> {
    protected String name;
    protected String comment;
    protected Map<String, String> properties;
    protected AuditInfo auditInfo;
    protected Column[] columns;
    protected Transform[] partitioning;
    protected SortOrder[] sortOrders;

    protected Distribution distribution;
    protected Namespace namespace;

    @Override
    public SELF withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return self();
    }

    /**
     * Sets the name of the table.
     *
     * @param name The name of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withName(String name) {
      this.name = name;
      return self();
    }

    /**
     * Sets the comment of the table.
     *
     * @param comment The comment or description for the table.
     * @return The builder instance.
     */
    @Override
    public SELF withComment(String comment) {
      this.comment = comment;
      return self();
    }

    /**
     * Sets the columns that make up the table.
     *
     * @param columns The columns that make up the table.
     * @return The builder instance.
     */
    @Override
    public SELF withColumns(Column[] columns) {
      this.columns = columns;
      return self();
    }

    /**
     * Sets the associated properties of the table.
     *
     * @param properties The associated properties of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withProperties(Map<String, String> properties) {
      this.properties = properties;
      return self();
    }

    /**
     * Sets the audit details of the table.
     *
     * @param auditInfo The audit details of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withAuditInfo(AuditInfo auditInfo) {
      this.auditInfo = auditInfo;
      return self();
    }

    /**
     * Sets the partitioning strategies of the table.
     *
     * @param partitioning The partitioning strategies of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withPartitioning(Transform[] partitioning) {
      this.partitioning = partitioning;
      return self();
    }

    public SELF withSortOrders(SortOrder[] sortOrders) {
      this.sortOrders = sortOrders;
      return (SELF) this;
    }

    public SELF withDistribution(Distribution distribution) {
      this.distribution = distribution;
      return (SELF) this;
    }

    /**
     * Builds the instance of the table with the provided attributes.
     *
     * @return The built table instance.
     */
    @Override
    public T build() {
      T t = internalBuild();
      return t;
    }

    private SELF self() {
      return (SELF) this;
    }

    protected abstract T internalBuild();
  }
}
