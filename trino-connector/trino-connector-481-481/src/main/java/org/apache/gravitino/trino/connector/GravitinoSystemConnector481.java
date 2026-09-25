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
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import org.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;

/** The Trino 481 variant of the Gravitino system connector. */
public class GravitinoSystemConnector481 extends GravitinoSystemConnector {

  /**
   * Constructs a new GravitinoSystemConnector481.
   *
   * <p>gravitinoStoredProcedureFactory the factory for creating stored procedures
   * systemTableFactory the registry of system tables to expose
   */
  public GravitinoSystemConnector481(
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory,
      GravitinoSystemTableFactory systemTableFactory) {
    super(gravitinoStoredProcedureFactory, systemTableFactory);
  }

  @Override
  protected ConnectorSplitManager createSplitManager() {
    return new SystemSplitManager481();
  }

  @Override
  protected ConnectorPageSourceProvider createPageSourceProvider() {
    return new DatasourceProvider481(getSystemTableFactory());
  }

  static class DatasourceProvider481 extends DatasourceProvider {

    DatasourceProvider481(GravitinoSystemTableFactory systemTableFactory) {
      super(systemTableFactory);
    }

    @Override
    protected ConnectorPageSource createPageSource(Page page) {
      return new SystemTablePageSource481(page);
    }
  }

  static class SystemSplitManager481 extends SplitManager {

    protected ConnectorSplit createSplit(SchemaTableName tableName) {
      return new Split481(tableName, Split.getCurrentCoordinatorAddress());
    }
  }

  static class SystemTablePageSource481 extends SystemTablePageSource {

    public SystemTablePageSource481(Page page) {
      super(page);
    }

    public SourcePage getNextSourcePage() {
      return SourcePage.create(nextPage());
    }
  }

  /** A Gravitino system-table split for Trino 481. */
  public static class Split481 extends Split {

    /**
     * Constructs a new Split481 with the specified table name and coordinator address.
     *
     * <p>tableName the system table this split reads coordinatorAddress the address of the Trino
     * coordinator
     */
    @JsonCreator
    public Split481(
        @JsonProperty("tableName") SchemaTableName tableName,
        @JsonProperty("coordinatorAddress") HostAddress coordinatorAddress) {
      super(tableName, coordinatorAddress);
    }
  }
}
