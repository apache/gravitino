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
package org.apache.gravitino;

import lombok.EqualsAndHashCode;
import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;
import org.apache.gravitino.rel.SupportsPartitions;

@EqualsAndHashCode(callSuper = true)
public class TestTable extends BaseTable {

  @Override
  protected TableOperations newOps() {
    return new TestTableOperations();
  }

  @Override
  public SupportsPartitions supportPartitions() throws UnsupportedOperationException {
    return (SupportsPartitions) ops();
  }

  public static class Builder extends BaseTable.BaseTableBuilder<Builder, TestTable> {

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected TestTable internalBuild() {
      TestTable table = new TestTable();
      table.name = name;
      table.comment = comment;
      table.properties = properties;
      table.columns = columns;
      table.auditInfo = auditInfo;
      table.distribution = distribution;
      table.sortOrders = sortOrders;
      table.partitioning = partitioning;
      table.indexes = indexes;
      table.proxyPlugin = proxyPlugin;
      return table;
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
