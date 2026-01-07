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
package org.apache.gravitino.trino.connector.system;

import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import java.util.List;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;
import org.apache.gravitino.trino.connector.system.table.GravitinoSystemTableFactory;

public class GravitinoSystemConnector478 extends GravitinoSystemConnector {

  public GravitinoSystemConnector478(
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory) {
    super(gravitinoStoredProcedureFactory);
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    return new DatasourceProvider();
  }

  public static class DatasourceProvider478 extends DatasourceProvider {
    @Override
    public ConnectorPageSource createPageSource(
        ConnectorTransactionHandle transaction,
        ConnectorSession session,
        ConnectorSplit split,
        ConnectorTableHandle table,
        List<ColumnHandle> columns,
        DynamicFilter dynamicFilter) {

      SchemaTableName tableName =
          ((GravitinoSystemConnectorMetadata.SystemTableHandle) table).getName();
      return new SystemTablePageSource478(GravitinoSystemTableFactory.loadPageData(tableName));
    }
  }

  public static class SystemTablePageSource478 extends SystemTablePageSource {
    public SystemTablePageSource478(Page page) {
      super(page);
    }

    @Override
    public SourcePage getNextSourcePage() {
      if (isFinished) {
        return null;
      }
      isFinished = true;
      return SourcePage.create(page);
    }
  }
}
