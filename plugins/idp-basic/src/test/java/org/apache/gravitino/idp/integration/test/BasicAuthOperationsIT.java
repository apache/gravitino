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
package org.apache.gravitino.idp.integration.test;

import static org.apache.gravitino.integration.test.util.BaseIT.setEnv;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.gravitino.Configs;
import org.apache.gravitino.auth.AuthenticatorType;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoVersion;
import org.apache.gravitino.idp.web.rest.feature.IdpRESTFeature;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.ITUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Integration tests for Gravitino client HTTP Basic authentication against the REST server. */
public class BasicAuthOperationsIT extends BaseIT {

  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "Passw0rd-For-Admin1";

  public void setGravitinoAdminClient(GravitinoAdminClient client) {
    this.client = client;
  }

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    setEnv(IdpRESTFeature.INITIAL_ADMIN_PASSWORD_ENV, ADMIN_PASSWORD);
    Map<String, String> configs = Maps.newHashMap();
    configs.put(Configs.ENABLE_AUTHORIZATION.getKey(), String.valueOf(false));
    configs.put(Configs.CACHE_ENABLED.getKey(), String.valueOf(false));
    configs.put(Configs.STORE_DELETE_AFTER_TIME.getKey(), String.valueOf(20 * 60 * 1000L));
    configs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    configs.put(Configs.AUTHENTICATORS.getKey(), AuthenticatorType.BASIC.name().toLowerCase());
    configs.put(
        Configs.REST_API_EXTENSION_PACKAGES.getKey(), IdpRESTFeature.IDP_REST_EXTENSION_PACKAGE);
    registerCustomConfigs(configs);
    super.startIntegrationTest();

    client = GravitinoAdminClient.builder(serverUri).withBasicAuth(ADMIN, ADMIN_PASSWORD).build();
  }

  @AfterAll
  public void stopIntegrationTest() throws IOException, InterruptedException {
    super.stopIntegrationTest();
  }

  @Test
  public void testAuthenticationApi() throws Exception {
    GravitinoVersion gravitinoVersion = client.serverVersion();
    Assertions.assertEquals(System.getenv("PROJECT_VERSION"), gravitinoVersion.version());
    Assertions.assertFalse(gravitinoVersion.compileDate().isEmpty());
    if (testMode.equals(ITUtils.EMBEDDED_TEST_MODE)) {
      final String gitCommitId = readGitCommitIdFromGitFile();
      Assertions.assertEquals(gitCommitId, gravitinoVersion.gitCommit());
    }
  }
}
