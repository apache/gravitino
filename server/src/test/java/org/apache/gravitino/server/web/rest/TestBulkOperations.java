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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.BulkRoleCreateRequest;
import org.apache.gravitino.dto.requests.GroupNamesRequest;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.requests.RoleNamesRequest;
import org.apache.gravitino.dto.requests.UserNamesRequest;
import org.apache.gravitino.dto.responses.BulkOperationResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
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
    when(ownerDispatcher.getOwner(any(), any())).thenReturn(Optional.empty());
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
    resourceConfig.register(BulkUserOperations.class);
    resourceConfig.register(BulkGroupOperations.class);
    resourceConfig.register(BulkRoleOperations.class);
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
    when(manager.addUser(eq("metalake1"), eq("user1"))).thenReturn(buildUser("user1"));
    doThrow(new UserAlreadyExistsException("mock error"))
        .when(manager)
        .addUser(eq("metalake1"), eq("user2"));

    Response resp =
        target("/bulk/metalakes/metalake1/users/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    Map.of("userNames", new String[] {"user1", "user2"}),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"user1"}, response.getSucceeded());
    Assertions.assertEquals(1, response.getFailed().length);
    Assertions.assertEquals("user2", response.getFailed()[0].getName());
  }

  @Test
  void testBulkRemoveUsersWithOwnerGuard() {
    Owner owner = mock(Owner.class);
    when(owner.type()).thenReturn(Owner.Type.USER);
    when(owner.name()).thenReturn("user1");
    when(ownerDispatcher.getOwner(any(), any())).thenReturn(Optional.of(owner));
    when(manager.removeUser(eq("metalake1"), eq("user2"))).thenReturn(true);

    Response resp =
        target("/bulk/metalakes/metalake1/users/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new UserNamesRequest(new String[] {"user1", "user2"}),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"user2"}, response.getSucceeded());
    Assertions.assertEquals("user1", response.getFailed()[0].getName());
  }

  @Test
  void testBulkAddGroups() {
    when(manager.addGroup(eq("metalake1"), eq("group1"))).thenReturn(buildGroup("group1"));
    when(manager.addGroup(eq("metalake1"), eq("group2"))).thenReturn(buildGroup("group2"));

    Response resp =
        target("/bulk/metalakes/metalake1/groups/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    Map.of("groupNames", new String[] {"group1", "group2"}),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"group1", "group2"}, response.getSucceeded());
    Assertions.assertEquals(0, response.getFailed().length);
  }

  @Test
  void testBulkRemoveGroupsWithPartialFailure() {
    when(manager.removeGroup(eq("metalake1"), eq("group1"))).thenReturn(true);
    when(manager.removeGroup(eq("metalake1"), eq("group2"))).thenReturn(false);

    Response resp =
        target("/bulk/metalakes/metalake1/groups/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new GroupNamesRequest(new String[] {"group1", "group2"}),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"group1"}, response.getSucceeded());
    Assertions.assertEquals("group2", response.getFailed()[0].getName());
  }

  @Test
  void testBulkAddRoles() {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    RoleCreateRequest roleRequest =
        new RoleCreateRequest(
            "role1",
            Collections.emptyMap(),
            new SecurableObjectDTO[] {DTOConverters.toDTO(securableObject)});
    when(manager.createRole(eq("metalake1"), eq("role1"), any(), any()))
        .thenReturn(buildRole("role1"));

    Response resp =
        target("/bulk/metalakes/metalake1/roles/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new BulkRoleCreateRequest(new RoleCreateRequest[] {roleRequest}),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"role1"}, response.getSucceeded());
    Assertions.assertEquals(0, response.getFailed().length);
  }

  @Test
  void testBulkRemoveRoles() {
    when(manager.deleteRole(eq("metalake1"), eq("role1"))).thenReturn(true);
    when(manager.deleteRole(eq("metalake1"), eq("role2"))).thenReturn(false);

    Response resp =
        target("/bulk/metalakes/metalake1/roles/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new RoleNamesRequest(new String[] {"role1", "role2"}),
                    MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    BulkOperationResponse response = resp.readEntity(BulkOperationResponse.class);
    Assertions.assertArrayEquals(new String[] {"role1"}, response.getSucceeded());
    Assertions.assertEquals("role2", response.getFailed()[0].getName());
  }

  private BaseMetalake inUseMetalake() {
    BaseMetalake metalake = mock(BaseMetalake.class);
    PropertiesMetadata propertiesMetadata = mock(PropertiesMetadata.class);
    when(propertiesMetadata.getOrDefault(any(), any())).thenReturn(true);
    when(metalake.propertiesMetadata()).thenReturn(propertiesMetadata);
    return metalake;
  }

  private User buildUser(String name) {
    return UserEntity.builder()
        .withId(1L)
        .withName(name)
        .withRoleNames(Collections.emptyList())
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private Group buildGroup(String name) {
    return GroupEntity.builder()
        .withId(1L)
        .withName(name)
        .withRoleNames(Collections.emptyList())
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private Role buildRole(String name) {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    return RoleEntity.builder()
        .withId(1L)
        .withName(name)
        .withProperties(Collections.emptyMap())
        .withSecurableObjects(Lists.newArrayList(catalog))
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }
}
