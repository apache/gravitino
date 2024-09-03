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
package org.apache.gravitino.client;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.authorization.GroupDTO;
import org.apache.gravitino.dto.authorization.UserDTO;
import org.apache.gravitino.dto.requests.RoleGrantRequest;
import org.apache.gravitino.dto.requests.RoleRevokeRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.GroupResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.UserResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPermission extends TestBase {

  private static final String metalakeName = "testMetalake";
  private static final String API_PERMISSION_PATH = "/api/metalakes/%s/permissions/%s";
  private static GravitinoClient gravitinoClient;

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();

    TestGravitinoMetalake.createMetalake(client, metalakeName);

    MetalakeDTO mockMetalake =
        MetalakeDTO.builder()
            .withName(metalakeName)
            .withComment("comment")
            .withAudit(
                AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    MetalakeResponse resp = new MetalakeResponse(mockMetalake);
    buildMockResource(Method.GET, "/api/metalakes/" + metalakeName, null, resp, HttpStatus.SC_OK);

    gravitinoClient =
        GravitinoClient.builder("http://127.0.0.1:" + mockServer.getLocalPort())
            .withMetalake(metalakeName)
            .withVersionCheckDisabled()
            .build();
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
    User grantedUser = gravitinoClient.grantRolesToUser(roles, user);
    Assertions.assertEquals(grantedUser.roles(), userDTO.roles());
    Assertions.assertEquals(grantedUser.name(), userDTO.name());

    // test Exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.PUT, userPath, request, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.grantRolesToUser(roles, user));
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
    User revokedUser = gravitinoClient.revokeRolesFromUser(roles, user);
    Assertions.assertEquals(revokedUser.roles(), userDTO.roles());
    Assertions.assertEquals(revokedUser.name(), userDTO.name());

    // test Exception
    ErrorResponse errResp2 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.PUT, userPath, null, errResp2, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.revokeRolesFromUser(roles, user));
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
    Group grantedGroup = gravitinoClient.grantRolesToGroup(roles, group);
    Assertions.assertEquals(grantedGroup.roles(), groupDTO.roles());
    Assertions.assertEquals(grantedGroup.name(), groupDTO.name());

    // test Exception
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, groupPath, request, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.grantRolesToGroup(roles, group));
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
    Group revokedGroup = gravitinoClient.revokeRolesFromGroup(roles, group);
    Assertions.assertEquals(revokedGroup.roles(), groupDTO.roles());
    Assertions.assertEquals(revokedGroup.name(), groupDTO.name());

    // test Exception
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, groupPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.revokeRolesFromGroup(roles, group));
  }
}
