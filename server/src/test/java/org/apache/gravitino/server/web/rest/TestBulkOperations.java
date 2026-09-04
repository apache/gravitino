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
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.apache.gravitino.authorization.OwnerDispatcher;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.bulk.BulkItemResult;
import org.apache.gravitino.bulk.BulkManager;
import org.apache.gravitino.bulk.GroupAdd;
import org.apache.gravitino.bulk.UserAdd;
import org.apache.gravitino.config.ConfigEntry;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.dto.requests.BulkGroupAddRequest;
import org.apache.gravitino.dto.requests.BulkRemoveRequest;
import org.apache.gravitino.dto.requests.BulkUserAddRequest;
import org.apache.gravitino.dto.requests.GroupAddRequest;
import org.apache.gravitino.dto.requests.UserAddRequest;
import org.apache.gravitino.dto.responses.BulkGroupResponse;
import org.apache.gravitino.dto.responses.BulkRemoveResponse;
import org.apache.gravitino.dto.responses.BulkUserResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestBulkOperations extends BaseOperationsTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);
  private static final EntityStore entityStore = mock(EntityStore.class);
  private static final OwnerDispatcher ownerDispatcher = mock(OwnerDispatcher.class);
  private static BulkOperations bulkOperations;

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config =
        mock(
            Config.class,
            invocation -> {
              if ("get".equals(invocation.getMethod().getName())
                  && invocation.getArguments().length == 1
                  && invocation.getArgument(0) instanceof ConfigEntry) {
                ConfigEntry<?> entry = invocation.getArgument(0);
                return entry.getDefaultValue();
              }
              return RETURNS_DEFAULTS.answer(invocation);
            });
    doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    doReturn(2).when(config).get(org.apache.gravitino.Configs.BULK_MAX_ITEMS);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlDispatcher", manager, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "internalOwnerDispatcher", ownerDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "bulkManager", new BulkManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "entityStore", entityStore, true);
    bulkOperations = new BulkOperations();
  }

  @BeforeEach
  public void resetMocks() throws IOException {
    Mockito.reset(manager, entityStore, ownerDispatcher);
    BaseMetalake metalake = mock(BaseMetalake.class);
    PropertiesMetadata propertiesMetadata = mock(PropertiesMetadata.class);
    when(propertiesMetadata.getOrDefault(any(), any())).thenReturn(true);
    when(metalake.propertiesMetadata()).thenReturn(propertiesMetadata);
    when(entityStore.get(any(), any(), any())).thenReturn(metalake);
    when(ownerDispatcher.getOwner(any(), any())).thenReturn(Optional.empty());
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
    resourceConfig.register(bulkOperations);
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
  public void testBulkAddUsersWithNullRequest() {
    Response resp =
        target("/bulk/metalakes/metalake1/users/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(new byte[0], MediaType.APPLICATION_JSON_TYPE));

    assertNullRequestBodyRejected(resp);
  }

  @Test
  public void testBulkAddUsersBestEffort() {
    User user1 = buildUser("user1");
    when(manager.addUsers(any(), any()))
        .thenReturn(
            Arrays.asList(
                BulkItemResult.success(0, "user1", user1),
                BulkItemResult.failure(
                    1, "user2", new UserAlreadyExistsException("User already exists: user2"))));

    BulkUserAddRequest request =
        new BulkUserAddRequest(
            new UserAddRequest[] {new UserAddRequest("user1"), new UserAddRequest("user2")});
    Response response =
        target("/bulk/metalakes/metalake1/users/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    BulkUserResponse bulkResponse = response.readEntity(BulkUserResponse.class);
    Assertions.assertEquals(1, bulkResponse.getUsers().length);
    Assertions.assertEquals("user1", bulkResponse.getUsers()[0].name());
    Assertions.assertEquals(1, bulkResponse.getErrors().length);
    Assertions.assertEquals(1, bulkResponse.getErrors()[0].getIndex());
    Assertions.assertEquals("user2", bulkResponse.getErrors()[0].getName());
    Assertions.assertEquals(
        ErrorConstants.ALREADY_EXISTS_CODE, bulkResponse.getErrors()[0].getCode());
    Assertions.assertEquals(2, bulkResponse.getSummary().getTotal());
    Assertions.assertEquals(1, bulkResponse.getSummary().getSucceeded());
    Assertions.assertEquals(1, bulkResponse.getSummary().getFailed());

    ArgumentCaptor<List<UserAdd>> usersCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(manager).addUsers(eq("metalake1"), usersCaptor.capture());
    Assertions.assertEquals("user1", usersCaptor.getValue().get(0).name());
  }

  @Test
  public void testBulkRemoveUsersBestEffort() {
    when(manager.removeUsers(any(), any(), any()))
        .thenReturn(
            Arrays.asList(
                BulkItemResult.success(0, "user1"),
                BulkItemResult.failure(
                    1, "ghost", new NoSuchUserException("User does not exist: ghost"))));

    BulkRemoveRequest request = new BulkRemoveRequest(new String[] {"user1", "ghost"});
    Response response =
        target("/bulk/metalakes/metalake1/users/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    BulkRemoveResponse bulkResponse = response.readEntity(BulkRemoveResponse.class);
    Assertions.assertArrayEquals(new String[] {"user1"}, bulkResponse.getNames());
    Assertions.assertEquals(1, bulkResponse.getErrors().length);
    Assertions.assertEquals("ghost", bulkResponse.getErrors()[0].getName());
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, bulkResponse.getErrors()[0].getCode());
  }

  @Test
  public void testBulkAddGroupsWithNullRequest() {
    Response resp =
        target("/bulk/metalakes/metalake1/groups/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(new byte[0], MediaType.APPLICATION_JSON_TYPE));

    assertNullRequestBodyRejected(resp);
  }

  @Test
  public void testBulkAddGroupsBestEffort() {
    Group group1 = buildGroup("group1");
    when(manager.addGroups(any(), any()))
        .thenReturn(
            Arrays.asList(
                BulkItemResult.success(0, "group1", group1),
                BulkItemResult.failure(
                    1, "group2", new GroupAlreadyExistsException("Group already exists: group2"))));

    BulkGroupAddRequest request =
        new BulkGroupAddRequest(
            new GroupAddRequest[] {new GroupAddRequest("group1"), new GroupAddRequest("group2")});
    Response response =
        target("/bulk/metalakes/metalake1/groups/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    BulkGroupResponse bulkResponse = response.readEntity(BulkGroupResponse.class);
    Assertions.assertEquals(1, bulkResponse.getGroups().length);
    Assertions.assertEquals("group1", bulkResponse.getGroups()[0].name());
    Assertions.assertEquals(1, bulkResponse.getErrors().length);
    Assertions.assertEquals(1, bulkResponse.getErrors()[0].getIndex());
    Assertions.assertEquals("group2", bulkResponse.getErrors()[0].getName());
    Assertions.assertEquals(
        ErrorConstants.ALREADY_EXISTS_CODE, bulkResponse.getErrors()[0].getCode());
    Assertions.assertEquals(2, bulkResponse.getSummary().getTotal());
    Assertions.assertEquals(1, bulkResponse.getSummary().getSucceeded());
    Assertions.assertEquals(1, bulkResponse.getSummary().getFailed());

    ArgumentCaptor<List<GroupAdd>> groupsCaptor = ArgumentCaptor.forClass(List.class);
    Mockito.verify(manager).addGroups(eq("metalake1"), groupsCaptor.capture());
    Assertions.assertEquals("group1", groupsCaptor.getValue().get(0).name());
  }

  @Test
  public void testBulkRemoveGroupsBestEffort() {
    when(manager.removeGroups(any(), any(), any()))
        .thenReturn(
            Arrays.asList(
                BulkItemResult.success(0, "group1"),
                BulkItemResult.failure(
                    1, "ghost", new NoSuchGroupException("Group does not exist: ghost"))));

    BulkRemoveRequest request = new BulkRemoveRequest(new String[] {"group1", "ghost"});
    Response response =
        target("/bulk/metalakes/metalake1/groups/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    BulkRemoveResponse bulkResponse = response.readEntity(BulkRemoveResponse.class);
    Assertions.assertArrayEquals(new String[] {"group1"}, bulkResponse.getNames());
    Assertions.assertEquals(1, bulkResponse.getErrors().length);
    Assertions.assertEquals("ghost", bulkResponse.getErrors()[0].getName());
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, bulkResponse.getErrors()[0].getCode());
  }

  @Test
  public void testRemoveUsersWithNullRequest() {
    Response response =
        target("/bulk/metalakes/metalake1/users/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity("null", MediaType.APPLICATION_JSON_TYPE));

    assertNullRequestBodyRejected(response);
  }

  @Test
  public void testRemoveGroupsWithNullRequest() {
    Response response =
        target("/bulk/metalakes/metalake1/groups/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(new byte[0], MediaType.APPLICATION_JSON_TYPE));

    assertNullRequestBodyRejected(response);
  }

  @Test
  public void testBulkRejectsEmptyAndExceededRequest() {
    Response emptyResponse =
        target("/bulk/metalakes/metalake1/users/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new BulkUserAddRequest(new UserAddRequest[] {}),
                    MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), emptyResponse.getStatus());

    BulkRemoveRequest exceededRequest =
        new BulkRemoveRequest(new String[] {"user1", "user2", "user3"});
    Response exceededResponse =
        target("/bulk/metalakes/metalake1/users/remove")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(exceededRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), exceededResponse.getStatus());
    ErrorResponse errorResponse = exceededResponse.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse.getCode());

    Response emptyGroupResponse =
        target("/bulk/metalakes/metalake1/groups/add")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(
                Entity.entity(
                    new BulkGroupAddRequest(new GroupAddRequest[] {}),
                    MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), emptyGroupResponse.getStatus());
  }

  private User buildUser(String user) {
    return UserEntity.builder()
        .withId(1L)
        .withName(user)
        .withRoleNames(Collections.emptyList())
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private Group buildGroup(String group) {
    return GroupEntity.builder()
        .withId(1L)
        .withName(group)
        .withRoleNames(Collections.emptyList())
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }
}
