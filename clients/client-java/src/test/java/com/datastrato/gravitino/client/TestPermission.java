/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.authorization.GroupDTO;
import com.datastrato.gravitino.dto.authorization.UserDTO;
import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.requests.RoleRevokeRequest;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPermission extends TestBase {

  private static final String metalakeName = "testMetalake";
  private static final String API_PERMISSION_PATH = "/api/metalakes/%s/permissions/%s";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
  }

  @Test
  public void testGrantRolesToUser() throws Exception {
    List<String> roles = Lists.newArrayList("role");
    String user = "user";
    String userPath =
        String.format(API_PERMISSION_PATH, metalakeName, String.format("users/%s/grant", user));
    RoleGrantRequest request = new RoleGrantRequest(roles);
    UserDTO userDTO =
        UserDTO.builder()
            .withName("user")
            .withRoles(Lists.newArrayList("roles"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    UserResponse response = new UserResponse(userDTO);

    buildMockResource(Method.PUT, userPath, request, response, SC_OK);
    User grantedUser = client.grantRolesToUser(metalakeName, roles, user);
    Assertions.assertEquals(grantedUser.roles(), userDTO.roles());
    Assertions.assertEquals(grantedUser.name(), userDTO.name());

    // test Exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.PUT, userPath, request, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.grantRolesToUser(metalakeName, roles, user));
  }

  @Test
  public void testRevokeRolesFromUser() throws Exception {
    List<String> roles = Lists.newArrayList("role");
    String user = "user";
    String userPath =
        String.format(API_PERMISSION_PATH, metalakeName, String.format("users/%s/revoke", user));
    UserDTO userDTO =
        UserDTO.builder()
            .withName("user")
            .withRoles(Lists.newArrayList())
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    UserResponse response = new UserResponse(userDTO);
    RoleRevokeRequest request = new RoleRevokeRequest(roles);

    buildMockResource(Method.PUT, userPath, request, response, SC_OK);
    User revokedUser = client.revokeRolesFromUser(metalakeName, roles, user);
    Assertions.assertEquals(revokedUser.roles(), userDTO.roles());
    Assertions.assertEquals(revokedUser.name(), userDTO.name());

    // test Exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.PUT, userPath, null, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.revokeRolesFromUser(metalakeName, roles, user));
  }

  @Test
  public void testGrantRolesToGroup() throws Exception {
    List<String> roles = Lists.newArrayList("role");
    String group = "group";
    String groupPath =
        String.format(API_PERMISSION_PATH, metalakeName, String.format("groups/%s/grant", group));
    RoleGrantRequest request = new RoleGrantRequest(roles);
    GroupDTO groupDTO =
        GroupDTO.builder()
            .withName("group")
            .withRoles(Lists.newArrayList("roles"))
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    GroupResponse response = new GroupResponse(groupDTO);

    buildMockResource(Method.PUT, groupPath, request, response, SC_OK);
    Group grantedGroup = client.grantRolesToGroup(metalakeName, roles, group);
    Assertions.assertEquals(grantedGroup.roles(), groupDTO.roles());
    Assertions.assertEquals(grantedGroup.name(), groupDTO.name());

    // test Exception
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, groupPath, request, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.grantRolesToGroup(metalakeName, roles, group));
  }

  @Test
  public void testRevokeRoleFromGroup() throws Exception {
    List<String> roles = Lists.newArrayList("role");
    String group = "group";
    String groupPath =
        String.format(API_PERMISSION_PATH, metalakeName, String.format("groups/%s/revoke", group));
    GroupDTO groupDTO =
        GroupDTO.builder()
            .withName("group")
            .withRoles(Lists.newArrayList())
            .withAudit(AuditDTO.builder().withCreator("test").withCreateTime(Instant.now()).build())
            .build();
    GroupResponse response = new GroupResponse(groupDTO);
    RoleRevokeRequest request = new RoleRevokeRequest(roles);

    buildMockResource(Method.PUT, groupPath, request, response, SC_OK);
    Group revokedGroup = client.revokeRolesFromGroup(metalakeName, roles, group);
    Assertions.assertEquals(revokedGroup.roles(), groupDTO.roles());
    Assertions.assertEquals(revokedGroup.name(), groupDTO.name());

    // test Exception
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, groupPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.revokeRolesFromGroup(metalakeName, roles, group));
  }
}
