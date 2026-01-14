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
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import org.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;

public class GravitinoSystemConnector473 extends GravitinoSystemConnector {

  public GravitinoSystemConnector473(
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory) {
    super(gravitinoStoredProcedureFactory);
  }

  @Override
  protected ConnectorSplitManager createSplitManager() {
    return new GravitinoSplitManager473();
  }

  @Override
  protected ConnectorPageSourceProvider createPageSourceProvider() {
    return new GravitinoPageSourceProvider473();
  }

  static class GravitinoSplitManager473 extends SplitManager {

    protected ConnectorSplit createSplit(SchemaTableName tableName) {
      return new Split473(tableName);
    }
  }

  static class GravitinoPageSourceProvider473 extends DatasourceProvider {

    @Override
    protected ConnectorPageSource createPageSource(Page page) {
      return new GravitinoPageSource473(page);
    }
  }

  public static class Split473 extends Split {

    @JsonCreator
    public Split473(@JsonProperty("tableName") SchemaTableName tableName) {
      super(tableName);
    }
  }

  static class GravitinoPageSource473 extends SystemTablePageSource {

    public GravitinoPageSource473(Page page) {
      super(page);
    }

    @SuppressWarnings("removal")
    public Page getNextPage() {
      return nextPage();
    }

    @Override
    public SourcePage getNextSourcePage() {
      Page page = nextPage();
      if (page == null) {
        return null;
      }
      return SourcePage.create(page);
    }
  }
}
