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

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.auth.AuthConstants;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.integration.test.util.BaseIT;
import org.apache.gravitino.integration.test.util.GravitinoITUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;

/**
 * Verifies that the Lance REST service authenticates to the Gravitino server as its configured
 * identity rather than anonymously.
 *
 * <p>Before the service was given an {@code AuthDataProvider}, its requests to the Gravitino server
 * carried no authorization header and were recorded against the anonymous user. Objects created
 * through the Lance REST service therefore had {@code anonymous} as their creator.
 */
public class LanceRESTServiceAuthIT extends BaseIT {

  private static final String SIMPLE_USER_NAME = "lance_rest_service_user";
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
  public void testCatalogCreatedViaLanceRestIsNotAnonymous() {
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

  private String getLanceRestServiceUrl() {
    return String.format("http://%s:%d/lance", "localhost", getLanceRESTServerPort());
  }
}
