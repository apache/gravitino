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

import io.trino.plugin.memory.MemoryPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import java.util.concurrent.TimeUnit;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorManager;
import org.awaitility.Awaitility;

abstract class AbstractGravitinoConnectorTest extends AbstractTestQueryFramework {

  GravitinoMockServer server;
  int trinoVersion;

  @Override
  protected QueryRunner createQueryRunner() throws Exception {
    GravitinoAdminClient gravitinoClient = createGravitinoClient();
    try {

      DistributedQueryRunner queryRunner = createTrinoQueryRunner();

      GravitinoPlugin gravitinoPlugin = createGravitinoPulgin(gravitinoClient);
      queryRunner.installPlugin(gravitinoPlugin);

      configureCatalogs(queryRunner, gravitinoClient);

      GravitinoConnectorPluginManager.instance(this.getClass().getClassLoader())
          .installPlugin("memory", new MemoryPlugin());
      CatalogConnectorManager catalogConnectorManager =
          gravitinoPlugin.getCatalogConnectorManager();
      server.setCatalogConnectorManager(catalogConnectorManager);
      trinoVersion = gravitinoPlugin.getTrinoVersion();

      // Wait for the catalog to be created. Wait for at least 30 seconds.
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .pollInterval(1, TimeUnit.SECONDS)
          .until(() -> !catalogConnectorManager.getCatalogs().isEmpty());

      return queryRunner;
    } catch (Exception e) {
      throw new RuntimeException("Create query runner failed", e);
    }
  }

  protected GravitinoPlugin createGravitinoPulgin(GravitinoAdminClient client) {
    return new GravitinoPlugin(client);
  }

  protected abstract DistributedQueryRunner createTrinoQueryRunner() throws Exception;

  protected GravitinoAdminClient createGravitinoClient() {
    server = closeAfterClass(new GravitinoMockServer());
    return server.createGravitinoClient();
  }

  protected abstract void configureCatalogs(
      DistributedQueryRunner queryRunner, GravitinoAdminClient gravitinoClient) throws Exception;
}
