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
package org.apache.gravitino.connector;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.ToString;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;

/** An abstract class representing a base table in a relational database. */
@Evolving
@ToString
public abstract class BaseTable implements Table {

  protected String name;

  @Nullable protected String comment;

  @Nullable protected Map<String, String> properties;

  protected AuditInfo auditInfo;

  protected Column[] columns;

  @Nullable protected Transform[] partitioning;

  @Nullable protected SortOrder[] sortOrders;

  @Nullable protected Distribution distribution;

  @Nullable protected Index[] indexes;

  protected Optional<ProxyPlugin> proxyPlugin;

  private volatile TableOperations ops;

  /**
   * @return The {@link TableOperations} instance associated with this table.
   * @throws UnsupportedOperationException if the table does not support operations.
   */
  @Evolving
  protected abstract TableOperations newOps() throws UnsupportedOperationException;

  /**
   * Retrieves the {@link TableOperations} instance associated with this table. If the instance is
   * not initialized, it is initialized and returned.
   *
   * @return The {@link TableOperations} instance associated with this table.
   */
  public TableOperations ops() {
    if (ops == null) {
      synchronized (this) {
        if (ops == null) {
          TableOperations newOps = newOps();
          ops =
              proxyPlugin.map(plugin -> OperationsProxy.createProxy(newOps, plugin)).orElse(newOps);
        }
      }
    }

    return ops;
  }

  /**
   * @return the audit details of the table.
   */
  @Override
  public AuditInfo auditInfo() {
    return auditInfo;
  }

  /**
   * @return the name of the table.
   */
  @Override
  public String name() {
    return name;
  }

  /**
   * @return an array of columns that make up the table.
   */
  @Override
  public Column[] columns() {
    return columns;
  }

  /**
   * @return the comment or description for the table.
   */
  @Nullable
  @Override
  public String comment() {
    return comment;
  }

  /**
   * @return the associated properties of the table.
   */
  @Override
  public Map<String, String> properties() {
    return properties;
  }

  /**
   * @return the partitioning strategies of the table.
   */
  @Override
  public Transform[] partitioning() {
    return partitioning;
  }

  /**
   * @return the array of {@link SortOrder} of the table.
   */
  @Override
  public SortOrder[] sortOrder() {
    return sortOrders;
  }

  /**
   * @return The distribution strategy of the table.
   */
  @Override
  public Distribution distribution() {
    return distribution;
  }

  /**
   * @return The indexes associated with the table.
   */
  @Override
  public Index[] index() {
    return indexes;
  }

  /**
   * Builder interface for creating instances of {@link BaseTable}.
   *
   * @param <SELF> The type of the builder.
   * @param <T> The type of the table being built.
   */
  interface Builder<SELF extends Builder<SELF, T>, T extends BaseTable> {

    SELF withName(String name);

    SELF withColumns(Column[] columns);

    SELF withComment(String comment);

    SELF withProperties(Map<String, String> properties);

    SELF withAuditInfo(AuditInfo auditInfo);

    SELF withPartitioning(Transform[] partitioning);

    SELF withSortOrders(SortOrder[] sortOrders);

    SELF withDistribution(Distribution distribution);

    SELF withIndexes(Index[] indexes);

    SELF withProxyPlugin(ProxyPlugin plugin);

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
    protected Index[] indexes;
    protected Optional<ProxyPlugin> proxyPlugin = Optional.empty();

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

    /**
     * Sets the array of {@link SortOrder} of the table.
     *
     * @param sortOrders The array of {@link SortOrder} of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withSortOrders(SortOrder[] sortOrders) {
      this.sortOrders = sortOrders;
      return (SELF) this;
    }

    /**
     * Sets the distribution strategy of the table.
     *
     * @param distribution The distribution strategy of the table.
     * @return The builder instance.
     */
    @Override
    public SELF withDistribution(Distribution distribution) {
      this.distribution = distribution;
      return (SELF) this;
    }

    /**
     * Sets the indexes associated with the table.
     *
     * @param indexes The indexes associated with the table.
     * @return The builder instance.
     */
    @Override
    public SELF withIndexes(Index[] indexes) {
      this.indexes = indexes;
      return (SELF) this;
    }

    /**
     * Sets the proxy plugin for the table.
     *
     * @param proxyPlugin The proxy plugin for the table.
     * @return The builder instance.
     */
    @Override
    public SELF withProxyPlugin(ProxyPlugin proxyPlugin) {
      this.proxyPlugin = Optional.ofNullable(proxyPlugin);
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

    /**
     * Builds the concrete instance of the table with the provided attributes.
     *
     * @return The built table instance.
     */
    @Evolving
    protected abstract T internalBuild();
  }
}
