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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorFactory;
import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;

/** Trino plugin endpoint, using java spi mechanism */
public class GravitinoPlugin477 extends GravitinoPlugin {

  private List<GravitinoConnectorFactory> factorys = new ArrayList<>();

  public GravitinoPlugin477(GravitinoAdminClient client) {
    super(client);
  }

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    GravitinoConnectorFactory factory = createConnectorFactory(client);
    factorys.add(factory);
    return ImmutableList.of(factory);
  }

  @Override
  protected GravitinoConnectorFactory createConnectorFactory(GravitinoAdminClient client) {
    return new GravitinoConnectorFactory477(client);
  }

  @VisibleForTesting
  @Override
  CatalogConnectorManager getCatalogConnectorManager() {
    for (GravitinoConnectorFactory factory : factorys) {
      if (factory.getCatalogConnectorManager() != null) {
        return factory.getCatalogConnectorManager();
      }
    }
    throw new RuntimeException("Failed to get CatalogConnectorManager");
  }

  @VisibleForTesting
  @Override
  int getTrinoVersion() {
    for (GravitinoConnectorFactory factory : factorys) {
      if (factory.getCatalogConnectorManager() != null) {
        return factory.getTrinoVersion();
      }
    }
    throw new RuntimeException("Failed to get TrinoVersion");
  }
}
