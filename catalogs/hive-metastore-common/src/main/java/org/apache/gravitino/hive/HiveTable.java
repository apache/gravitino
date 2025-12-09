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
package org.apache.gravitino.hive;

import static org.apache.gravitino.catalog.hive.HiveConstants.COMMENT;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Optional;
import java.util.Set;
import lombok.ToString;
import org.apache.gravitino.catalog.hive.TableType;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.ProxyPlugin;
import org.apache.gravitino.connector.TableOperations;

/** Represents an Apache Hive Table entity in the Hive Metastore catalog. */
@ToString
public class HiveTable extends BaseTable {

  // A set of supported Hive table types.
  public static final Set<String> SUPPORT_TABLE_TYPES =
      Sets.newHashSet(TableType.MANAGED_TABLE.name(), TableType.EXTERNAL_TABLE.name());
  public static final String ICEBERG_TABLE_TYPE_VALUE = "ICEBERG";
  public static final String TABLE_TYPE_PROP = "table_type";
  private String catalogName;
  private String databaseName;

  protected HiveTable() {}

  public String catalogName() {
    return catalogName;
  }

  public String databaseName() {
    return databaseName;
  }

  public void setProxyPlugin(ProxyPlugin plugin) {
    this.proxyPlugin = Optional.ofNullable(plugin);
  }

  @Override
  protected TableOperations newOps() throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  /** A builder class for constructing HiveTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, HiveTable> {

    private String catalogName;
    private String databaseName;

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Sets the catalog name of the HiveTable.
     *
     * @param catalogName The catalog name of the HiveTable.
     * @return This Builder instance.
     */
    public Builder withCatalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    /**
     * Sets the Hive schema (database) name to be used for building the HiveTable.
     *
     * @param databaseName The string database name of the HiveTable.
     * @return This Builder instance.
     */
    public Builder withDatabaseName(String databaseName) {
      this.databaseName = databaseName;
      return this;
    }

    /**
     * Internal method to build a HiveTable instance using the provided values.
     *
     * @return A new HiveTable instance with the configured values.
     */
    @Override
    protected HiveTable internalBuild() {
      HiveTable hiveTable = new HiveTable();
      hiveTable.name = name;
      hiveTable.comment = comment;
      hiveTable.properties = properties != null ? Maps.newHashMap(properties) : Maps.newHashMap();
      hiveTable.auditInfo = auditInfo;
      hiveTable.columns = columns;
      hiveTable.distribution = distribution;
      hiveTable.sortOrders = sortOrders;
      hiveTable.partitioning = partitioning;
      hiveTable.catalogName = catalogName;
      hiveTable.databaseName = databaseName;
      hiveTable.proxyPlugin = proxyPlugin;

      // HMS put table comment in parameters
      if (comment != null) {
        hiveTable.properties.put(COMMENT, comment);
      }

      return hiveTable;
    }
  }
}
