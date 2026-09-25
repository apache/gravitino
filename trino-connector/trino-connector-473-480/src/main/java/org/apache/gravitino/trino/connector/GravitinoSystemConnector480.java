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

/** The Trino 480+ variant of the Gravitino system connector. */
public class GravitinoSystemConnector480 extends GravitinoSystemConnector {

  /**
   * Constructs a new GravitinoSystemConnector480.
   *
   * @param gravitinoStoredProcedureFactory the factory for creating stored procedures
   * @param systemTableFactory the registry of system tables to expose
   */
  public GravitinoSystemConnector480(
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory,
      GravitinoSystemTableFactory systemTableFactory) {
    super(gravitinoStoredProcedureFactory, systemTableFactory);
  }

  @Override
  protected ConnectorSplitManager createSplitManager() {
    return new GravitinoSplitManager480();
  }

  @Override
  protected ConnectorPageSourceProvider createPageSourceProvider() {
    return new DatasourceProvider480(getSystemTableFactory());
  }

  static class DatasourceProvider480 extends DatasourceProvider {

    DatasourceProvider480(GravitinoSystemTableFactory systemTableFactory) {
      super(systemTableFactory);
    }

    @Override
    protected ConnectorPageSource createPageSource(Page page) {
      return new SystemTablePageSource480(page);
    }
  }

  static class GravitinoSplitManager480 extends SplitManager {

    protected ConnectorSplit createSplit(SchemaTableName tableName) {
      return new Split480(tableName, Split.getCurrentCoordinatorAddress());
    }
  }

  static class SystemTablePageSource480 extends SystemTablePageSource {

    public SystemTablePageSource480(Page page) {
      super(page);
    }

    public SourcePage getNextSourcePage() {
      return SourcePage.create(nextPage());
    }
  }

  public static class Split480 extends Split {

    @JsonCreator
    public Split480(
        @JsonProperty("tableName") SchemaTableName tableName,
        @JsonProperty("coordinatorAddress") HostAddress coordinatorAddress) {
      super(tableName, coordinatorAddress);
    }
  }
}
