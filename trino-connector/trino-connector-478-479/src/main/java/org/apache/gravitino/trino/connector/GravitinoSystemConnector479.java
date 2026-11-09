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
import org.apache.gravitino.trino.connector.system.SystemDatasourceProvider;
import org.apache.gravitino.trino.connector.system.SystemSplitManager;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;

/** The Trino 478-479 variant of the Gravitino system connector. */
public class GravitinoSystemConnector479 extends GravitinoSystemConnector {

  /**
   * Constructs a new GravitinoSystemConnector479.
   *
   * @param gravitinoStoredProcedureFactory the factory for creating stored procedures
   * @param systemTableFactory the registry of system tables to expose
   */
  public GravitinoSystemConnector479(
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory,
      GravitinoSystemTableFactory systemTableFactory) {
    super(gravitinoStoredProcedureFactory, systemTableFactory);
  }

  @Override
  protected ConnectorSplitManager createSplitManager() {
    return new GravitinoSplitManager479();
  }

  @Override
  protected ConnectorPageSourceProvider createPageSourceProvider() {
    return new DatasourceProvider479(getSystemTableFactory());
  }

  static class DatasourceProvider479 extends SystemDatasourceProvider {

    DatasourceProvider479(GravitinoSystemTableFactory systemTableFactory) {
      super(systemTableFactory);
    }

    @Override
    protected ConnectorPageSource createPageSource(Page page) {
      return new SystemTablePageSource479(page);
    }
  }

  static class GravitinoSplitManager479 extends SystemSplitManager {

    protected ConnectorSplit createSplit(SchemaTableName tableName) {
      return new Split479(tableName, Split.getCurrentCoordinatorAddress());
    }
  }

  static class SystemTablePageSource479 extends SystemTablePageSource {

    public SystemTablePageSource479(Page page) {
      super(page);
    }

    public SourcePage getNextSourcePage() {
      return SourcePage.create(nextPage());
    }
  }

  public static class Split479 extends Split {

    @JsonCreator
    public Split479(
        @JsonProperty("tableName") SchemaTableName tableName,
        @JsonProperty("coordinatorAddress") HostAddress coordinatorAddress) {
      super(tableName, coordinatorAddress);
    }
  }
}
