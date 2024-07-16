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

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import io.trino.Session;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import java.util.HashMap;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
public class TestCreateGravitinoConnector {

  GravitinoMockServer server;

  @Test
  public void testCreateConnectorsWithEnableSimpleCatalog() throws Exception {
    server = new GravitinoMockServer();
    Session session = testSessionBuilder().setCatalog("gravitino").build();
    QueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();
    TestGravitinoPlugin gravitinoPlugin = new TestGravitinoPlugin(gravitinoClient);
    queryRunner.installPlugin(gravitinoPlugin);

    // test create two connector and set gravitino.simplify-catalog-names = true
    {
      // create a gravitino connector named gravitino using metalake test
      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test");
      properties.put("gravitino.uri", "http://127.0.0.1:8090");
      properties.put("gravitino.simplify-catalog-names", "true");
      queryRunner.createCatalog("test0", "gravitino", properties);
    }

    {
      // Test failed to create catalog with different metalake
      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test1");
      properties.put("gravitino.uri", "http://127.0.0.1:8090");
      properties.put("gravitino.simplify-catalog-names", "true");
      try {
        queryRunner.createCatalog("test1", "gravitino", properties);
      } catch (Exception e) {
        assertThat(e.getMessage()).contains("Multiple metalakes are not supported");
      }
    }

    server.close();
  }

  @Test
  public void testCreateConnectorsWithDisableSimpleCatalog() throws Exception {
    server = new GravitinoMockServer();
    Session session = testSessionBuilder().setCatalog("gravitino").build();
    QueryRunner queryRunner = DistributedQueryRunner.builder(session).setNodeCount(1).build();

    GravitinoAdminClient gravitinoClient = server.createGravitinoClient();
    TestGravitinoPlugin gravitinoPlugin = new TestGravitinoPlugin(gravitinoClient);
    queryRunner.installPlugin(gravitinoPlugin);

    // test create two connector and set gravitino.simplify-catalog-names = false
    {
      // create a gravitino connector named gravitino using metalake test
      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test");
      properties.put("gravitino.uri", "http://127.0.0.1:8090");
      properties.put("gravitino.simplify-catalog-names", "false");
      queryRunner.createCatalog("test0", "gravitino", properties);
    }

    {
      // Test failed to create catalog with different metalake
      HashMap<String, String> properties = new HashMap<>();
      properties.put("gravitino.metalake", "test1");
      properties.put("gravitino.uri", "http://127.0.0.1:8090");
      properties.put("gravitino.simplify-catalog-names", "false");
      queryRunner.createCatalog("test1", "gravitino", properties);
    }

    server.close();
  }
}
