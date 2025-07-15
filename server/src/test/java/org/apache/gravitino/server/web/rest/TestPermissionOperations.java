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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.requests.PrivilegeGrantRequest;
import org.apache.gravitino.dto.requests.PrivilegeRevokeRequest;
import org.apache.gravitino.dto.requests.RoleGrantRequest;
import org.apache.gravitino.dto.requests.RoleRevokeRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.GroupResponse;
import org.apache.gravitino.dto.responses.RoleResponse;
import org.apache.gravitino.dto.responses.UserResponse;
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
import org.apache.gravitino.exceptions.IllegalRoleException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.GroupEntity;
import org.apache.gravitino.meta.RoleEntity;
import org.apache.gravitino.meta.UserEntity;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestPermissionOperations extends BaseOperationsTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);
  private static final MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);

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
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlDispatcher", manager, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "metalakeDispatcher", metalakeDispatcher, true);
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
    resourceConfig.register(PermissionOperations.class);
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
  public void testGrantRolesToUser() {
    UserEntity userEntity =
        UserEntity.builder()
            .withId(1L)
            .withName("user")
            .withRoleNames(Lists.newArrayList("roles"))
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.grantRolesToUser(any(), any(), any())).thenReturn(userEntity);

    RoleGrantRequest illegalReq = new RoleGrantRequest(null);
    Response illegalResp =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    RoleGrantRequest request = new RoleGrantRequest(Lists.newArrayList("role1"));
    Response resp =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    UserResponse userResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, userResponse.getCode());
    User user = userResponse.getUser();
    Assertions.assertEquals(userEntity.roles(), user.roles());
    Assertions.assertEquals(userEntity.name(), user.name());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(manager)
        .grantRolesToUser(any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error"))
        .when(manager)
        .grantRolesToUser(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    // Test to throw IllegalRoleException
    doThrow(new IllegalRoleException("mock error"))
        .when(manager)
        .grantRolesToUser(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(IllegalRoleException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).grantRolesToUser(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/users/user/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testGrantRolesToGroup() {
    GroupEntity groupEntity =
        GroupEntity.builder()
            .withId(1L)
            .withName("group")
            .withRoleNames(Lists.newArrayList("roles"))
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.grantRolesToGroup(any(), any(), any())).thenReturn(groupEntity);

    // Test with Illegal request
    RoleGrantRequest illegalReq = new RoleGrantRequest(null);
    Response illegalResp =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    RoleGrantRequest request = new RoleGrantRequest(Lists.newArrayList("role1"));

    Response resp =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    GroupResponse grantResponse = resp.readEntity(GroupResponse.class);
    Assertions.assertEquals(0, grantResponse.getCode());

    Group group = grantResponse.getGroup();
    Assertions.assertEquals(groupEntity.roles(), group.roles());
    Assertions.assertEquals(groupEntity.name(), group.name());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchUserException
    doThrow(new NoSuchUserException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchUserException.class.getSimpleName(), errorResponse.getType());

    // Test to throw IllegalRoleException
    doThrow(new IllegalRoleException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    resp1 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(IllegalRoleException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .grantRolesToGroup(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/groups/group/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testRevokeRolesFromUser() {
    UserEntity userEntity =
        UserEntity.builder()
            .withId(1L)
            .withName("user")
            .withRoleNames(Lists.newArrayList())
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.revokeRolesFromUser(any(), any(), any())).thenReturn(userEntity);

    // Test with illegal request
    RoleRevokeRequest illegalReq = new RoleRevokeRequest(null);
    Response illegalResp =
        target("/metalakes/metalake1/permissions/users/user1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    RoleRevokeRequest request = new RoleRevokeRequest(Lists.newArrayList("role1"));

    Response resp =
        target("/metalakes/metalake1/permissions/users/user1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    UserResponse revokeResponse = resp.readEntity(UserResponse.class);
    Assertions.assertEquals(0, revokeResponse.getCode());

    User user = revokeResponse.getUser();
    Assertions.assertEquals(userEntity.roles(), user.roles());
    Assertions.assertEquals(userEntity.name(), user.name());

    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .revokeRolesFromUser(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/users/user1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw IllegalRoleException
    doThrow(new IllegalRoleException("mock error"))
        .when(manager)
        .revokeRolesFromUser(any(), any(), any());
    Response nsrResponse =
        target("/metalakes/metalake1/permissions/users/user/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nsrResponse.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, nsrResponse.getMediaType());

    errorResponse = nsrResponse.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(IllegalRoleException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testRevokeRolesFromGroup() {
    GroupEntity groupEntity =
        GroupEntity.builder()
            .withId(1L)
            .withName("group")
            .withRoleNames(Lists.newArrayList())
            .withRoleIds(Lists.newArrayList(1L))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.revokeRolesFromGroup(any(), any(), any())).thenReturn(groupEntity);
    // Test with illegal request
    RoleRevokeRequest illegalReq = new RoleRevokeRequest(null);
    Response illegalResp =
        target("/metalakes/metalake1/permissions/groups/group1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    RoleRevokeRequest request = new RoleRevokeRequest(Lists.newArrayList("role1"));

    Response resp =
        target("/metalakes/metalake1/permissions/groups/group1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    GroupResponse revokeResponse = resp.readEntity(GroupResponse.class);
    Assertions.assertEquals(0, revokeResponse.getCode());

    Group group = revokeResponse.getGroup();
    Assertions.assertEquals(groupEntity.roles(), group.roles());
    Assertions.assertEquals(groupEntity.name(), group.name());

    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .revokeRolesFromGroup(any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/groups/group1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw IllegalRoleException
    doThrow(new IllegalRoleException("mock error"))
        .when(manager)
        .revokeRolesFromGroup(any(), any(), any());
    Response nsrResponse =
        target("/metalakes/metalake1/permissions/groups/group/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), nsrResponse.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, nsrResponse.getMediaType());

    errorResponse = nsrResponse.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse.getCode());
    Assertions.assertEquals(IllegalRoleException.class.getSimpleName(), errorResponse.getType());
  }

  @Test
  public void testGrantPrivilegesToRole() {
    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(1L)
            .withName("role")
            .withSecurableObjects(
                Lists.newArrayList(
                    SecurableObjects.ofMetalake(
                        "metalake1", Lists.newArrayList(Privileges.CreateTable.allow()))))
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.grantPrivilegeToRole(any(), any(), any(), any())).thenReturn(roleEntity);
    when(metalakeDispatcher.metalakeExists(any())).thenReturn(true);
    PrivilegeGrantRequest request =
        new PrivilegeGrantRequest(
            Lists.newArrayList(
                PrivilegeDTO.builder()
                    .withName(Privilege.Name.CREATE_TABLE)
                    .withCondition(Privilege.Condition.ALLOW)
                    .build()));

    Response resp =
        target("/metalakes/metalake1/permissions/roles/role1/metalake/metalake1/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    RoleResponse grantResponse = resp.readEntity(RoleResponse.class);
    Assertions.assertEquals(0, grantResponse.getCode());

    Role role = grantResponse.getRole();
    Assertions.assertEquals(roleEntity.name(), role.name());
    Assertions.assertEquals(1, role.securableObjects().size());
    Assertions.assertEquals("metalake1", role.securableObjects().get(0).name());
    Assertions.assertEquals(
        Privilege.Name.CREATE_TABLE, role.securableObjects().get(0).privileges().get(0).name());
    Assertions.assertEquals(
        Privilege.Condition.ALLOW, role.securableObjects().get(0).privileges().get(0).condition());
    Assertions.assertEquals(roleEntity.properties(), role.properties());

    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .grantPrivilegeToRole(any(), any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/roles/role1/metalake/metalake1/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());

    // Test with wrong binding privileges
    PrivilegeGrantRequest wrongPriRequest =
        new PrivilegeGrantRequest(
            Lists.newArrayList(
                PrivilegeDTO.builder()
                    .withName(Privilege.Name.CREATE_CATALOG)
                    .withCondition(Privilege.Condition.ALLOW)
                    .build()));

    Response wrongPrivilegeResp =
        target("/metalakes/metalake1/permissions/roles/role1/catalog/catalog1/grant")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(wrongPriRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), wrongPrivilegeResp.getStatus());

    ErrorResponse wrongPriErrorResp = wrongPrivilegeResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, wrongPriErrorResp.getCode());
    Assertions.assertEquals(
        IllegalPrivilegeException.class.getSimpleName(), wrongPriErrorResp.getType());
  }

  @Test
  public void testRevokePrivilegesFromRole() {
    RoleEntity roleEntity =
        RoleEntity.builder()
            .withId(1L)
            .withName("role")
            .withSecurableObjects(Lists.newArrayList())
            .withAuditInfo(
                AuditInfo.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    when(manager.revokePrivilegesFromRole(any(), any(), any(), any())).thenReturn(roleEntity);
    when(metalakeDispatcher.metalakeExists(any())).thenReturn(true);

    // Test with illegal request
    PrivilegeRevokeRequest illegalReq = new PrivilegeRevokeRequest(null);
    Response illegalResp =
        target("/metalakes/metalake1/permissions/roles/role1/metalake/metalake1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(illegalReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    PrivilegeRevokeRequest request =
        new PrivilegeRevokeRequest(
            Lists.newArrayList(
                PrivilegeDTO.builder()
                    .withName(Privilege.Name.CREATE_TABLE)
                    .withCondition(Privilege.Condition.ALLOW)
                    .build()));

    Response resp =
        target("/metalakes/metalake1/permissions/roles/role1/metalake/metalake1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    RoleResponse revokeResponse = resp.readEntity(RoleResponse.class);
    Assertions.assertEquals(0, revokeResponse.getCode());

    Role role = revokeResponse.getRole();
    Assertions.assertEquals(roleEntity.name(), role.name());
    Assertions.assertEquals(roleEntity.securableObjects(), role.securableObjects());
    Assertions.assertEquals(roleEntity.properties(), role.properties());

    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .revokePrivilegesFromRole(any(), any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/permissions/roles/role1/metalake/metalake1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());

    // Test with wrong binding privileges
    PrivilegeRevokeRequest wrongPriRequest =
        new PrivilegeRevokeRequest(
            Lists.newArrayList(
                PrivilegeDTO.builder()
                    .withName(Privilege.Name.CREATE_CATALOG)
                    .withCondition(Privilege.Condition.ALLOW)
                    .build()));

    Response wrongPrivilegeResp =
        target("/metalakes/metalake1/permissions/roles/role1/catalog/catalog1/revoke")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(wrongPriRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), wrongPrivilegeResp.getStatus());

    ErrorResponse wrongPriErrorResp = wrongPrivilegeResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, wrongPriErrorResp.getCode());
    Assertions.assertEquals(
        IllegalPrivilegeException.class.getSimpleName(), wrongPriErrorResp.getType());
  }
}
