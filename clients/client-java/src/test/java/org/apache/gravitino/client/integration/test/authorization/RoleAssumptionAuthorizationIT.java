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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.exceptions.RESTException;
import org.apache.gravitino.integration.test.container.ContainerSuite;
import org.apache.gravitino.integration.test.container.HiveContainer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for role assumption via the {@code X-Gravitino-Active-Roles} header. The test
 * user holds two roles, each granting {@code USE_CATALOG} on a different catalog, and the header is
 * used to activate a subset of them per request. Covers: default {@code ALL}, a narrowed {@code
 * NAMED} subset, {@code NONE}, a {@code 403} for an unheld role, and a {@code 400} for a malformed
 * header.
 */
@Tag("gravitino-docker-test")
public class RoleAssumptionAuthorizationIT extends BaseRestApiAuthorizationIT {

  private static final String CATALOG1 = "catalog1";
  private static final String CATALOG2 = "catalog2";
  private static final String ROLE_CATALOG1 = "role_catalog1";
  private static final String ROLE_CATALOG2 = "role_catalog2";

  private static final ContainerSuite containerSuite = ContainerSuite.getInstance();
  private static String hmsUri;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    containerSuite.startHiveContainer();
    super.startIntegrationTest();
    hmsUri =
        String.format(
            "thrift://%s:%d",
            containerSuite.getHiveContainer().getContainerIpAddress(),
            HiveContainer.HIVE_METASTORE_PORT);

    Map<String, String> properties = Maps.newHashMap();
    properties.put("metastore.uris", hmsUri);
    GravitinoMetalake admin = client.loadMetalake(METALAKE);
    admin.createCatalog(CATALOG1, Catalog.Type.RELATIONAL, "hive", "comment", properties);
    admin.createCatalog(CATALOG2, Catalog.Type.RELATIONAL, "hive", "comment", properties);

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

  private static String[] sortedCatalogs(GravitinoAdminClient userClient) {
    String[] catalogs = userClient.loadMetalake(METALAKE).listCatalogs();
    Arrays.sort(catalogs);
    return catalogs;
  }

  @Test
  public void testAllRolesActiveByDefault() {
    // No header (or ALL) activates every held role, so both catalogs are visible.
    assertEquals(2, sortedCatalogs(normalUserClient).length);
    try (GravitinoAdminClient all = userClientWithActiveRoles("ALL")) {
      assertEquals(2, sortedCatalogs(all).length);
    }
  }

  @Test
  public void testNarrowToSingleRole() {
    // Activating only the catalog1 role narrows the visible catalogs to catalog1.
    try (GravitinoAdminClient onlyCatalog1 = userClientWithActiveRoles(ROLE_CATALOG1)) {
      String[] catalogs = sortedCatalogs(onlyCatalog1);
      assertEquals(1, catalogs.length);
      assertEquals(CATALOG1, catalogs[0]);
    }
    try (GravitinoAdminClient onlyCatalog2 = userClientWithActiveRoles(ROLE_CATALOG2)) {
      String[] catalogs = sortedCatalogs(onlyCatalog2);
      assertEquals(1, catalogs.length);
      assertEquals(CATALOG2, catalogs[0]);
    }
  }

  @Test
  public void testNoneActivatesNothing() {
    // NONE activates no role, so no catalog is visible.
    try (GravitinoAdminClient none = userClientWithActiveRoles("NONE")) {
      assertEquals(0, sortedCatalogs(none).length);
    }
  }

  @Test
  public void testUnheldRoleReturns403() {
    // Declaring a role the caller does not hold is rejected before authorization.
    try (GravitinoAdminClient unheld = userClientWithActiveRoles("role_not_held")) {
      assertThrows(ForbiddenException.class, () -> unheld.loadMetalake(METALAKE).listCatalogs());
    }
  }

  @Test
  public void testMalformedHeaderReturns400() {
    // A reserved keyword combined with a role name is syntactically invalid -> 400. The server
    // rejects it in the authentication filter; the client surfaces the unmapped 400 as a
    // RESTException.
    try (GravitinoAdminClient malformed = userClientWithActiveRoles("ALL," + ROLE_CATALOG1)) {
      assertThrows(RESTException.class, () -> malformed.loadMetalake(METALAKE).listCatalogs());
    }
  }
}
