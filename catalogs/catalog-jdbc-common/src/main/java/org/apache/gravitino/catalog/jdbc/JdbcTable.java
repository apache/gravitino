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
package org.apache.gravitino.catalog.jdbc;

import com.google.common.collect.Maps;
import java.util.Map;
import lombok.ToString;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.gravitino.catalog.jdbc.operation.TableOperation;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.rel.SupportsPartitions;

/** Represents a Jdbc Table entity in the jdbc table. */
@ToString
public class JdbcTable extends BaseTable {
  private String databaseName;
  private TableOperation tableOperation;

  private JdbcTable() {}

  @Override
  protected TableOperations newOps() {
    if (ArrayUtils.isEmpty(partitioning)) {
      throw new UnsupportedOperationException(
          "Table partition operation is not supported for non-partitioned table: " + name);
    }
    return tableOperation.createJdbcTablePartitionOperations(this);
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return (SupportsPartitions) ops();
  }

  public String databaseName() {
    return databaseName;
  }

  /** A builder class for constructing JdbcTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, JdbcTable> {
    private String databaseName;
    private TableOperation tableOperation;

    /**
     * Sets the name of database.
     *
     * @param databaseName The name of database.
     * @return This Builder instance.
     */
    public Builder withDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    /**
     * Sets the table operation to be used for partition operations.
     *
     * @param tableOperation The instance of TableOperation.
     * @return This Builder instance.
     */
    public Builder withTableOperation(TableOperation tableOperation) {
      this.tableOperation = tableOperation;
      return this;
    }

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Internal method to build a JdbcTable instance using the provided values.
     *
     * @return A new JdbcTable instance with the configured values.
     */
    @Override
    protected JdbcTable internalBuild() {
      JdbcTable jdbcTable = new JdbcTable();
      jdbcTable.name = name;
      jdbcTable.comment = comment;
      jdbcTable.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      jdbcTable.auditInfo = auditInfo;
      jdbcTable.columns = columns;
      jdbcTable.partitioning = partitioning;
      jdbcTable.distribution = distribution;
      jdbcTable.sortOrders = sortOrders;
      jdbcTable.indexes = indexes;
      jdbcTable.proxyPlugin = proxyPlugin;
      jdbcTable.databaseName = databaseName;
      jdbcTable.tableOperation = tableOperation;
      return jdbcTable;
    }

    public String comment() {
      return comment;
    }

    public Map<String, String> properties() {
      return properties;
    }
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
