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

public class GravitinoSystemConnector478 extends GravitinoSystemConnector {

  public GravitinoSystemConnector478(
      GravitinoStoredProcedureFactory gravitinoStoredProcedureFactory) {
    super(gravitinoStoredProcedureFactory);
  }

  @Override
  protected ConnectorSplitManager createSplitManager() {
    return new GravitinoSplitManager478();
  }

  @Override
  protected ConnectorPageSourceProvider createPageSourceProvider() {
    return new DatasourceProvider478();
  }

  static class DatasourceProvider478 extends DatasourceProvider {

    @Override
    protected ConnectorPageSource createPageSource(Page page) {
      return new SystemTablePageSource478(page);
    }
  }

  static class GravitinoSplitManager478 extends SplitManager {

    protected ConnectorSplit createSplit(SchemaTableName tableName) {
      return new Split478(tableName);
    }
  }

  static class SystemTablePageSource478 extends SystemTablePageSource {

    public SystemTablePageSource478(Page page) {
      super(page);
    }

    public SourcePage getNextSourcePage() {
      return SourcePage.create(nextPage());
    }
  }

  public static class Split478 extends Split {

    @JsonCreator
    public Split478(@JsonProperty("tableName") SchemaTableName tableName) {
      super(tableName);
    }
  }
}
