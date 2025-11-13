/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.connector;

import org.apache.gravitino.annotation.DeveloperApi;

/** A generic table implementation that represents the table managed in Gravitino. */
@DeveloperApi
public class GenericTable extends BaseTable {

  /**
   * Creates a new builder for constructing a GenericTable instance.
   *
   * @return A new Builder instance.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * This method is not supported for GenericTable and will always throw an
   * UnsupportedOperationException.
   *
   * @throws UnsupportedOperationException always thrown to indicate that table operations are not
   *     supported.
   */
  @Override
  protected TableOperations newOps() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Generic Table doesn't support table operations");
  }

  /** A builder class for constructing GenericTable instances. */
  public static class Builder extends BaseTableBuilder<Builder, GenericTable> {
    /**
     * Internal method to build a GenericTable instance using the provided values.
     *
     * @return A new GenericTable instance with the configured values.
     */
    @Override
    protected GenericTable internalBuild() {
      GenericTable table = new GenericTable();
      table.columns = this.columns;
      table.comment = this.comment;
      table.properties = this.properties;
      table.auditInfo = this.auditInfo;
      table.distribution = this.distribution;
      table.indexes = this.indexes;
      table.name = this.name;
      table.partitioning = this.partitioning;
      table.sortOrders = this.sortOrders;
      return table;
    }
  }
}
