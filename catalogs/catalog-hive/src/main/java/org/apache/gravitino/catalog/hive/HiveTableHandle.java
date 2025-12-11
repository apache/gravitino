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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.catalog.hive.HiveConstants.TABLE_TYPE;

import lombok.ToString;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.hive.CachedClientPool;
import org.apache.gravitino.hive.HiveTable;
import org.apache.gravitino.rel.SupportsPartitions;

/** Represents an Apache Hive Table entity in the Hive Metastore catalog. */
@ToString
public class HiveTableHandle extends BaseTable {
  private final HiveTable table;
  private final CachedClientPool clientPool;

  public HiveTableHandle(HiveTable hiveTable, CachedClientPool clientPool) {
    this.table = hiveTable;
    this.clientPool = clientPool;
    this.name = hiveTable.name();
    this.comment = hiveTable.comment();
    this.properties = hiveTable.properties();
    this.auditInfo = hiveTable.auditInfo();
    this.columns = hiveTable.columns();
    this.partitioning = hiveTable.partitioning();
    this.sortOrders = hiveTable.sortOrder();
    this.distribution = hiveTable.distribution();
    this.indexes = hiveTable.index();
    this.proxyPlugin = hiveTable.proxyPlugin();
  }

  public HiveTable table() {
    return table;
  }

  public CachedClientPool clientPool() {
    return clientPool;
  }

  @Override
  protected TableOperations newOps() {
    return new HiveTableOperations(this);
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return (SupportsPartitions) ops();
  }

  public void close() {
    clientPool.close();
  }

  public String getTableType() {
    return properties.getOrDefault(TABLE_TYPE, "MANAGED_TABLE");
  }
}
