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

import static io.trino.testing.TestingSession.testSessionBuilder;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.trino.connector.GravitinoPlugin;
import org.apache.gravitino.trino.connector.GravitinoPlugin446;
import org.apache.gravitino.trino.connector.TestGravitinoConnector;
import org.apache.gravitino.trino.connector.TestGravitinoConnectorWithMetalakeCatalogName;
import org.junit.jupiter.api.Nested;

public class TestGravitinoConnector446 {
  @Nested
  class SingleMetalake extends TestGravitinoConnector {
    @Override
    protected GravitinoPlugin createGravitinoPlugin(GravitinoAdminClient client) {
      return new GravitinoPlugin446(client);
    }

    @Override
    protected DistributedQueryRunner createTrinoQueryRunner() throws Exception {
      Session session = testSessionBuilder().setCatalog("gravitino").build();
      return DistributedQueryRunner.builder(session).setWorkerCount(1).build();
    }
  }

  @Nested
  class MultiMetalake extends TestGravitinoConnectorWithMetalakeCatalogName {
    @Override
    protected GravitinoPlugin createGravitinoPlugin(GravitinoAdminClient client) {
      return new GravitinoPlugin446(client);
    }

    @Override
    protected DistributedQueryRunner createTrinoQueryRunner() throws Exception {
      Session session = testSessionBuilder().setCatalog("gravitino").build();
      return DistributedQueryRunner.builder(session).setWorkerCount(1).build();
    }
  }
}
