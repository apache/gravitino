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
package org.apache.gravitino.catalog.lakehouse.hudi;

import org.apache.gravitino.connector.BaseTable;
import org.apache.gravitino.connector.TableOperations;

/**
 * Represents a table from a Hudi catalog. Different Hudi catalog backends should extend this
 * abstract class and the inner Builder class to provide the conversion between the backend table
 * and the HudiTable.
 *
 * @param <TABLE> the table type from the backend
 */
public abstract class HudiTable<TABLE> extends BaseTable {
  /**
   * Converts the HudiTable to the backend table.
   *
   * @return the backend table
   */
  public abstract TABLE fromHudiTable();

  @Override
  public TableOperations newOps() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public abstract static class Builder<T> extends BaseTableBuilder<Builder<T>, HudiTable> {
    T backendTable;

    public Builder<T> withBackendTable(T backendTable) {
      this.backendTable = backendTable;
      return this;
    }

    @Override
    protected HudiTable<T> internalBuild() {
      return backendTable == null ? simpleBuild() : buildFromTable(backendTable);
    }

    /**
     * Builds a simple HudiTable without a backend table.
     *
     * @return the HudiTable
     */
    protected abstract HudiTable<T> simpleBuild();

    /**
     * Builds a HudiTable from the backend table.
     *
     * @param backendTable the backend table
     * @return the HudiTable
     */
    protected abstract HudiTable<T> buildFromTable(T backendTable);

    @Override
    public HudiTable<T> build() {
      return internalBuild();
    }
  }
}
