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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for role assumption via the {@code X-Gravitino-Active-Roles} header. The test
 * user holds two roles, each granting {@code USE_CATALOG} on a different catalog, and the header
 * activates a subset of them per request. Covers the default {@code ALL}, a narrowed {@code NAMED}
 * subset (one role and several), {@code NONE}, a {@code 403} for an unheld role, and a {@code 400}
 * for a malformed header.
 */
@Tag("gravitino-docker-test")
public class RoleAssumptionAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG1 = "catalog1";
  private static final String CATALOG2 = "catalog2";
  private static final String ROLE_CATALOG1 = "role_catalog1";
  private static final String ROLE_CATALOG2 = "role_catalog2";
  private static final String UNHELD_ROLE = "role_not_held";

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.startIntegrationTest();
    GravitinoMetalake admin = client.loadMetalake(METALAKE);
    // Model catalogs need no external system; only their visibility in listCatalogs matters here.
    admin.createCatalog(CATALOG1, Catalog.Type.MODEL, "model", "comment", new HashMap<>());
    admin.createCatalog(CATALOG2, Catalog.Type.MODEL, "model", "comment", new HashMap<>());

    // One role per catalog, each granting only USE_CATALOG on that catalog.
    SecurableObject useCatalog1 =
        SecurableObjects.ofCatalog(CATALOG1, ImmutableList.of(Privileges.UseCatalog.allow()));
    SecurableObject useCatalog2 =
        SecurableObjects.ofCatalog(CATALOG2, ImmutableList.of(Privileges.UseCatalog.allow()));
    admin.createRole(ROLE_CATALOG1, new HashMap<>(), ImmutableList.of(useCatalog1));
    admin.createRole(ROLE_CATALOG2, new HashMap<>(), ImmutableList.of(useCatalog2));
    admin.grantRolesToUser(ImmutableList.of(ROLE_CATALOG1, ROLE_CATALOG2), NORMAL_USER);
  }

  /** Builds a client for {@link #NORMAL_USER} that sends the given active-roles header value. */
  private GravitinoAdminClient userClientWithActiveRoles(String headerValue) {
    return GravitinoAdminClient.builder(serverUri)
        .withSimpleAuth(NORMAL_USER)
        .withHeaders(ImmutableMap.of(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, headerValue))
        .build();
  }

  private String[] sortedCatalogs(GravitinoAdminClient userClient) {
    String[] catalogs = userClient.loadMetalake(METALAKE).listCatalogs();
    Arrays.sort(catalogs);
    return catalogs;
  }

  @Test
  public void testAllRolesActiveByDefault() {
    // No header (or ALL) activates every held role, so both catalogs are visible.
    assertArrayEquals(new String[] {CATALOG1, CATALOG2}, sortedCatalogs(normalUserClient));
    try (GravitinoAdminClient all = userClientWithActiveRoles("ALL")) {
      assertArrayEquals(new String[] {CATALOG1, CATALOG2}, sortedCatalogs(all));
    }
  }

  @Test
  public void testNarrowToSingleRole() {
    // Activating a single role narrows the visible catalogs to that role's catalog.
    try (GravitinoAdminClient onlyCatalog1 = userClientWithActiveRoles(ROLE_CATALOG1)) {
      assertArrayEquals(new String[] {CATALOG1}, sortedCatalogs(onlyCatalog1));
    }
    try (GravitinoAdminClient onlyCatalog2 = userClientWithActiveRoles(ROLE_CATALOG2)) {
      assertArrayEquals(new String[] {CATALOG2}, sortedCatalogs(onlyCatalog2));
    }
  }

  @Test
  public void testNarrowToMultipleRoles() {
    // A comma-separated list activates every named role.
    try (GravitinoAdminClient both =
        userClientWithActiveRoles(ROLE_CATALOG1 + "," + ROLE_CATALOG2)) {
      assertArrayEquals(new String[] {CATALOG1, CATALOG2}, sortedCatalogs(both));
    }
  }

  @Test
  public void testNoneActivatesNothing() {
    // NONE activates no role, so no catalog is visible.
    try (GravitinoAdminClient none = userClientWithActiveRoles("NONE")) {
      assertArrayEquals(new String[0], sortedCatalogs(none));
    }
  }

  @Test
  public void testUnheldRoleReturns403() {
    // Declaring a role the caller does not hold is rejected before authorization.
    try (GravitinoAdminClient unheld = userClientWithActiveRoles(UNHELD_ROLE)) {
      ForbiddenException exception =
          assertThrows(
              ForbiddenException.class, () -> unheld.loadMetalake(METALAKE).listCatalogs());
      assertTrue(
          exception.getMessage().contains("cannot assume active role"),
          "Unexpected message: " + exception.getMessage());
      assertTrue(
          exception.getMessage().contains(UNHELD_ROLE),
          "Unexpected message: " + exception.getMessage());
    }
  }

  @Test
  public void testMalformedHeaderReturns400() throws Exception {
    // A reserved keyword combined with a role name is syntactically invalid. Sent over raw HTTP so
    // that the status code itself is asserted rather than the client's exception mapping.
    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString((NORMAL_USER + ":dummy").getBytes(StandardCharsets.UTF_8));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(new URI(serverUri + "/api/metalakes/" + METALAKE + "/catalogs"))
            .header(AuthConstants.HTTP_HEADER_AUTHORIZATION, authHeader)
            .header(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, "ALL," + ROLE_CATALOG1)
            .GET()
            .build();
    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

    assertEquals(400, response.statusCode(), "Unexpected body: " + response.body());
    assertTrue(
        response.body().contains(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER),
        "Unexpected body: " + response.body());
  }
}
