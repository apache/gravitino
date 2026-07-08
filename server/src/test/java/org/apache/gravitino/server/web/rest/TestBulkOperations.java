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
package org.apache.gravitino.server.web.rest;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.EntityStore;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.BulkOperationResult;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.dto.requests.BulkUserAddRequest;
import org.apache.gravitino.dto.requests.UserAddRequest;
import org.apache.gravitino.dto.requests.UsernamesRequest;
import org.apache.gravitino.dto.responses.BulkOperationResponse;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestBulkOperations extends BaseOperationsTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);
  private static final OwnerDispatcher ownerDispatcher = mock(OwnerDispatcher.class);
  private static final EntityStore entityStore = mock(EntityStore.class);
  private static final CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlDispatcher", manager, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", catalogDispatcher, true);
  }

  @BeforeEach
  void resetMocks() throws IOException {
    reset(manager, ownerDispatcher, entityStore, catalogDispatcher);
    BaseMetalake metalake = inUseMetalake();
    when(entityStore.get(any(), any(), any())).thenReturn(metalake);
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);
  }

  @Override
  protected Application configure() {
    try {
      forceSet(
          TestProperties.CONTAINER_PORT, String.valueOf(RESTUtils.findAvailablePort(2000, 3000)));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(UserOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  void testBulkAddUsers() {
    when(manager.bulkAddUsers(eq("metalake1"), any()))
        .thenReturn(
            new BulkOperationResult(
                new String[] {"user1"},
                new BulkOperationResult.Failure[] {
                  new BulkOperationResult.Failure("user2", "UserAlreadyExistsException: mock error")
                }));

    Response resp =
        target("/bulk/metalakes/metalake1/users/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new BulkUserAddRequest(
                        new UserAddRequest[] {
                          new UserAddRequest("user1"), new UserAddRequest("user2")
                        }),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"user1"}, response.getSucceeded());
    Assertions.assertEquals(1, response.getFailed().length);
    Assertions.assertEquals("user2", response.getFailed()[0].getName());
  }

  @Test
  void testBulkAddUsersWithExternalId() {
    when(manager.bulkAddUsers(eq("metalake1"), any()))
        .thenReturn(
            new BulkOperationResult(
                new String[] {"user1", "user2"}, new BulkOperationResult.Failure[0]));

    Response resp =
        target("/bulk/metalakes/metalake1/users/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new BulkUserAddRequest(
                        new UserAddRequest[] {
                          new UserAddRequest("user1", "external1", false),
                          new UserAddRequest("user2", "external2", null)
                        }),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"user1", "user2"}, response.getSucceeded());
    Assertions.assertEquals(0, response.getFailed().length);
  }

  @Test
  void testBulkRemoveUsersWithPartialFailure() {
    when(manager.bulkRemoveUsers(eq("metalake1"), any()))
        .thenReturn(
            new BulkOperationResult(
                new String[] {"user1"},
                new BulkOperationResult.Failure[] {
                  new BulkOperationResult.Failure(
                      "user2", "IllegalArgumentException: User does not exist")
                }));

    Response resp =
        target("/bulk/metalakes/metalake1/users/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new UsernamesRequest(new String[] {"user1", "user2"}),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"user1"}, response.getSucceeded());
    Assertions.assertEquals("user2", response.getFailed()[0].getName());
  }

  private BaseMetalake inUseMetalake() {
    BaseMetalake metalake = mock(BaseMetalake.class);
    PropertiesMetadata propertiesMetadata = mock(PropertiesMetadata.class);
    when(propertiesMetadata.getOrDefault(any(), any())).thenReturn(true);
    when(metalake.propertiesMetadata()).thenReturn(propertiesMetadata);
    return metalake;
  }
}
