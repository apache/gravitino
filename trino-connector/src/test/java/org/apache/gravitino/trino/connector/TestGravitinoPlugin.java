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

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorFactory;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;

public class TestGravitinoPlugin extends GravitinoPlugin {
  private TestGravitinoConnectorFactory factory;

  private final GravitinoAdminClient gravitinoClient;

  public TestGravitinoPlugin(GravitinoAdminClient gravitinoClient) {
    this.gravitinoClient = gravitinoClient;
  }

  @Override
  public Iterable<ConnectorFactory> getConnectorFactories() {
    factory = new TestGravitinoConnectorFactory();
    factory.setGravitinoClient(gravitinoClient);
    return ImmutableList.of(factory);
  }

  public CatalogConnectorManager getCatalogConnectorManager() {
    return factory.getCatalogConnectorManager();
  }
}
