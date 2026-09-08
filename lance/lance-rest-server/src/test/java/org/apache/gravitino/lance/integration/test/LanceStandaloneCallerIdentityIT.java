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
 *
 */
package org.apache.gravitino.lance.integration.test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Configs;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.lance.LanceRESTService;
import org.apache.gravitino.rest.RESTUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Exercises standalone Lance REST identity forwarding through HTTP to a Gravitino server. */
public class LanceStandaloneCallerIdentityIT extends BaseIT {

  private static final String ADMIN = "standalone_admin";
  private static final String ALICE = "standalone_alice";
  private static final String BOB = "standalone_bob";
  private static final String METALAKE = "standalone_identity";
  private static final String ROLE = "catalog_creator";

  private final HttpClient httpClient = HttpClient.newHttpClient();
  private LanceRESTService standalone;
  private GravitinoMetalake metalake;
  private int port;

  @BeforeAll
  @Override
  public void startIntegrationTest() throws Exception {
    customConfigs.put(Configs.ENABLE_AUTHORIZATION.getKey(), "true");
    customConfigs.put(Configs.SERVICE_ADMINS.getKey(), ADMIN);
    customConfigs.put(Configs.AUTHENTICATORS.getKey(), "simple");
    customConfigs.put("SimpleAuthUserName", ADMIN);
    super.startIntegrationTest();
    metalake = client.createMetalake(METALAKE, "Standalone caller identity", Map.of());
    metalake.addUser(ALICE);
    metalake.addUser(BOB);
    metalake.createRole(
        ROLE,
        Map.of(),
        List.of(
            SecurableObjects.ofMetalake(
                METALAKE, new ArrayList<>(List.of(Privileges.CreateCatalog.allow())))));
    metalake.grantRolesToUser(List.of(ROLE), ALICE);

    port = RESTUtils.findAvailablePort(20000, 30000);
    standalone = new LanceRESTService();
    // The Gravitino test server owns the process-wide authenticator. Standalone mode still uses
    // its HTTP client and bypasses the auxiliary metadata authorization interceptors.
    standalone.serviceInit(
        Map.of(
            "httpPort",
            Integer.toString(port),
            "host",
            "127.0.0.1",
            "gravitino-uri",
            serverUri,
            "gravitino-metalake",
            METALAKE,
            "gravitino-simple.user-name",
            ADMIN),
        false);
    standalone.serviceStart();
  }

  @AfterAll
  public void clean() throws Exception {
    try {
      if (standalone != null) {
        standalone.serviceStop();
      }
      if (client != null) {
        client.dropMetalake(METALAKE, true);
      }
    } finally {
      super.stopIntegrationTest();
    }
  }

  @Test
  public void testRemoteAuthorizationRolesAndOwnershipUseTheCaller() throws Exception {
    assertStatus(200, create(ALICE, ROLE, "alice_catalog"));
    Catalog catalog = metalake.loadCatalog("alice_catalog");
    Assertions.assertEquals(ALICE, catalog.auditInfo().creator());
    Assertions.assertEquals(
        ALICE,
        metalake
            .getOwner(MetadataObjects.of(null, "alice_catalog", MetadataObject.Type.CATALOG))
            .orElseThrow()
            .name());
    assertStatus(403, create(BOB, "ALL", "bob_denied"));
    assertStatus(403, create(ALICE, "NONE", "inactive_role_denied"));
    assertStatus(403, create(ALICE, "unassigned_role", "unassigned_role_denied"));
    // After a denied request, the same shared client must still use the next caller's roles.
    assertStatus(200, create(ALICE, "ALL", "alice_after_denial"));
  }

  @Test
  public void testAnonymousRequestDoesNotFallBackToPrivilegedServiceUser() throws Exception {
    assertStatus(401, create(null, "ALL", "anonymous_denied"));
    Assertions.assertFalse(List.of(metalake.listCatalogs()).contains("anonymous_denied"));
  }

  private HttpResponse<String> create(@Nullable String user, String roles, String catalog)
      throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    "http://127.0.0.1:" + port + "/lance/v1/namespace/" + catalog + "/create"))
            .header("Content-Type", "application/json")
            .header(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, roles);
    if (user != null) {
      request.header(
          AuthConstants.HTTP_HEADER_AUTHORIZATION,
          "Basic "
              + Base64.getEncoder()
                  .encodeToString((user + ":dummy").getBytes(StandardCharsets.UTF_8)));
    }
    return httpClient.send(
        request.POST(HttpRequest.BodyPublishers.ofString("{\"id\":[\"" + catalog + "\"]}")).build(),
        HttpResponse.BodyHandlers.ofString());
  }

  private void assertStatus(int expected, HttpResponse<String> response) {
    Assertions.assertEquals(expected, response.statusCode(), response.body());
  }
}
