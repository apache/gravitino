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

import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.apache.gravitino.trino.connector.system.GravitinoSystemConnector;
import org.apache.gravitino.trino.connector.system.storedprocedure.GravitinoStoredProcedureFactory;

public class GravitinoConnectorFactory466 extends GravitinoConnectorFactory {

  public GravitinoConnectorFactory466(GravitinoAdminClient client) {
    super(client);
  }

  @Override
  protected int getMinSupportTrinoSpiVersion() {
    return 466;
  }

  @Override
  protected int getMaxSupportTrinoSpiVersion() {
    return 467;
  }

  @Override
  protected String getTrinoCatalogName(String metalake, String catalog) {
    return "\"" + metalake + "." + catalog + "\"";
  }

  @Override
  protected boolean supportCatalogNameWithMetalake() {
    return false;
  }

  @Override
  protected GravitinoConnector createConnector(CatalogConnectorContext connectorContext) {
    return new GravitinoConnector466(connectorContext);
  }

  @Override
  protected GravitinoSystemConnector createSystemConnector(
      GravitinoStoredProcedureFactory storedProcedureFactory) {
    return new GravitinoSystemConnector466(storedProcedureFactory);
  }
}
