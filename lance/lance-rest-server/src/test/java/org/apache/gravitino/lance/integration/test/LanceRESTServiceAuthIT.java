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
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.apache.gravitino.server.web.ObjectMapperProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;

/**
 * Verifies the identity used by auxiliary-mode Lance REST metadata operations.
 *
 * <p>An authenticated request must run as its caller so authorization, ownership, and audit data
 * use the real user. A request without a user falls back to the configured service identity so
 * internal Gravitino calls never run anonymously.
 */
public class LanceRESTServiceAuthIT extends BaseIT {

  private static final String SIMPLE_USER_NAME = "lance_rest_service_user";
  private static final String REQUEST_USER_NAME = "lance_rest_request_user";
  private static final String USER_NAME_CONFIG_KEY =
      "gravitino.lance-rest.gravitino-simple.user-name";

  private final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
  private GravitinoMetalake metalake;
  private LanceNamespace ns;

  @BeforeAll
  public void startIntegrationTest() throws Exception {
    super.ignoreLanceAuxRestService = false;
    registerCustomConfigs(new HashMap<>(Map.of(USER_NAME_CONFIG_KEY, SIMPLE_USER_NAME)));
    super.startIntegrationTest();

    this.metalake =
        client.createMetalake(
            getLanceRESTServerMetalakeName(), "metalake for lance rest auth tests", null);

    Map<String, String> props = new HashMap<>();
    props.put("uri", getLanceRestServiceUrl());
    props.put("delimiter", ".");
    this.ns = LanceNamespace.connect("rest", props, allocator);
  }

  @AfterAll
  public void clean() throws Exception {
    Exception failure = null;

    try {
      if (client != null) {
        client.dropMetalake(getLanceRESTServerMetalakeName(), true);
      }
    } catch (Exception e) {
      failure = e;
    }

    try {
      allocator.close();
    } catch (Exception e) {
      failure = failure == null ? e : failure;
    }

    try {
      super.stopIntegrationTest();
    } catch (Exception e) {
      failure = failure == null ? e : failure;
    }

    if (failure != null) {
      throw failure;
    }
  }

  @Test
  public void testAnonymousRequestUsesConfiguredServiceIdentity() {
    String catalogName = GravitinoITUtils.genRandomName("lance_auth_catalog");

    CreateNamespaceRequest createNamespaceReq = new CreateNamespaceRequest();
    createNamespaceReq.addIdItem(catalogName);
    ns.createNamespace(createNamespaceReq);

    Catalog catalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(
        SIMPLE_USER_NAME,
        catalog.auditInfo().creator(),
        "The Lance REST service should act as its configured user, not "
            + AuthConstants.ANONYMOUS_USER);
    Assertions.assertNotEquals(AuthConstants.ANONYMOUS_USER, catalog.auditInfo().creator());

    metalake.dropCatalog(catalogName, true);
  }

  @Test
  public void testCatalogCreatedViaLanceRestUsesAuthenticatedCaller() throws Exception {
    String catalogName = GravitinoITUtils.genRandomName("lance_auth_caller_catalog");
    CreateNamespaceRequest createNamespaceReq = new CreateNamespaceRequest();
    createNamespaceReq.addIdItem(catalogName);

    String authHeader =
        AuthConstants.AUTHORIZATION_BASIC_HEADER
            + Base64.getEncoder()
                .encodeToString((REQUEST_USER_NAME + ":dummy").getBytes(StandardCharsets.UTF_8));
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    getLanceRestServiceUrl()
                        + "/v1/namespace/"
                        + catalogName
                        + "/create?delimiter=."))
            .header(AuthConstants.HTTP_HEADER_AUTHORIZATION, authHeader)
            .header(AuthConstants.X_GRAVITINO_ACTIVE_ROLES_HEADER, "NONE")
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    ObjectMapperProvider.objectMapper().writeValueAsString(createNamespaceReq)))
            .build();
    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

    Assertions.assertEquals(200, response.statusCode(), "Unexpected body: " + response.body());
    Catalog catalog = metalake.loadCatalog(catalogName);
    Assertions.assertEquals(
        REQUEST_USER_NAME,
        catalog.auditInfo().creator(),
        "An authenticated Lance REST request should not be replaced by the service identity");

    metalake.dropCatalog(catalogName, true);
  }

  private String getLanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }
}
