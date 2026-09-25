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
package org.apache.gravitino.trino.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import java.util.List;
import java.util.Optional;
import org.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;

/** The Trino 482+ variant of the Gravitino system connector. */
public class GravitinoSystemConnector482 extends GravitinoSystemConnector {

  /**
   * Constructs a new GravitinoSystemConnector482.
   *
   * @param gravitinoStoredProcedureFactory the factory for creating stored procedures
   * @param systemTableFactory the registry of system tables to expose
   */
  public GravitinoSystemConnector482(
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory,
      GravitinoSystemTableFactory systemTableFactory) {
    super(gravitinoStoredProcedureFactory, systemTableFactory);
  }

  @Override
  protected ConnectorSplitManager createSplitManager() {
    return new SystemSplitManager482();
  }

  @Override
  protected ConnectorPageSourceProvider createPageSourceProvider() {
    return new DatasourceProvider482(getSystemTableFactory());
  }

  static class DatasourceProvider482 extends DatasourceProvider {

    DatasourceProvider482(GravitinoSystemTableFactory systemTableFactory) {
      super(systemTableFactory);
    }

    // Trino 482 reworked createPageSource; delegate to the shared table-handle helper so system
    // tables keep loading.
    @Override
    public ConnectorPageSource createPageSource(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorSplit split,
        ConnectorTableHandle table,
        Optional<ConnectorTableCredentials> tableCredentials,
        List<ColumnHandle> columns,
        DynamicFilter dynamicFilter,
        MemoryContext memoryContext) {
      return createPageSource(table, columns);
    }

    @Override
    protected ConnectorPageSource createPageSource(Page page) {
      return new SystemTablePageSource482(page);
    }
  }

  static class SystemSplitManager482 extends SplitManager {

    @Override
    protected ConnectorSplit createSplit(SchemaTableName tableName) {
      return new Split482(tableName, Split.getCurrentCoordinatorAddress());
    }
  }

  static class SystemTablePageSource482 extends SystemTablePageSource {

    public SystemTablePageSource482(Page page) {
      super(page);
    }

    public SourcePage getNextSourcePage() {
      return SourcePage.create(nextPage());
    }
  }

  /** A Gravitino system-table split for Trino 482. */
  public static class Split482 extends Split {

    /**
     * Constructs a new Split482 with the specified table name and coordinator address.
     *
     * @param tableName the table name
     * @param coordinatorAddress the host and port of the Trino coordinator, null if unknown
     */
    @JsonCreator
    public Split482(
        @JsonProperty("tableName") SchemaTableName tableName,
        @JsonProperty("coordinatorAddress") HostAddress coordinatorAddress) {
      super(tableName, coordinatorAddress);
    }
  }
}
