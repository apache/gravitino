/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import java.io.IOException;
import java.util.HashMap;
import org.apache.gravitino.Configs;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public class BaseRestApiAuthorizationIT extends BaseIT {

  protected static final String METALAKE = "testMetalake";

  protected static final String USER = "tester";

  protected static final String NORMAL_USER = "tester2";

  /** Mock a normal user without permissions. */
  protected static GravitinoAdminClient normalUserClient;

  private static final Logger LOG = LoggerFactory.getLogger(BaseRestApiAuthorizationIT.class);

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    // Enable authorization
    customConfigs.putAll(
        ImmutableMap.of(
            "SimpleAuthUserName",
            USER,
            Configs.ENABLE_AUTHORIZATION.getKey(),
            "true",
            Configs.CACHE_ENABLED.getKey(),
            "false",
            Configs.AUTHENTICATORS.getKey(),
            "simple"));
    putServiceAdmin();
    super.startIntegrationTest();
    client.createMetalake(METALAKE, "", new HashMap<>());
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.addUser(NORMAL_USER);
    normalUserClient = GravitinoAdminClient.builder(serverUri).withSimpleAuth(NORMAL_USER).build();
  }

  protected void putServiceAdmin() {
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), USER);
  }

  @AfterAll
  @Override
  public void stopIntegrationTest() throws IOException, InterruptedException {
    client.dropMetalake(METALAKE, true);

    if (normalUserClient != null) {
      normalUserClient.close();
      normalUserClient = null;
    }

    try {
      closer.close();
    } catch (Exception e) {
      LOG.error("Exception in closing CloseableGroup", e);
    }
    super.stopIntegrationTest();
  }
}
