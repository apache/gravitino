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

import static javax.servlet.http.HttpServletResponse.SC_CONFLICT;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import java.time.Instant;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.authorization.GroupDTO;
import org.apache.gravitino.dto.authorization.UserDTO;
import org.apache.gravitino.dto.requests.GroupAddRequest;
import org.apache.gravitino.dto.requests.UserAddRequest;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.GroupResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.RemoveResponse;
import org.apache.gravitino.dto.responses.UserResponse;
import org.apache.gravitino.exceptions.GroupAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchGroupException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchUserException;
import org.apache.gravitino.exceptions.UserAlreadyExistsException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestUserGroup extends TestBase {

  private static final String API_METALAKES_USERS_PATH = "api/metalakes/%s/users/%s";
  private static final String API_METALAKES_GROUPS_PATH = "api/metalakes/%s/groups/%s";
  private static GravitinoClient gravitinoClient;
  private static final String metalakeName = "testMetalake";

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
  public void testAddUsers() throws Exception {
    String username = "user";
    String userPath = withSlash(String.format(API_METALAKES_USERS_PATH, metalakeName, ""));
    UserAddRequest request = new UserAddRequest(username);

    UserDTO mockUser = mockUserDTO(username);
    UserResponse userResponse = new UserResponse(mockUser);
    buildMockResource(Method.POST, userPath, request, userResponse, SC_OK);

    User addedUser = gravitinoClient.addUser(username);
    Assertions.assertNotNull(addedUser);
    assertUser(addedUser, mockUser);

    // test UserAlreadyExistsException
    ErrorResponse errResp1 =
        ErrorResponse.alreadyExists(
            UserAlreadyExistsException.class.getSimpleName(), "user already exists");
    buildMockResource(Method.POST, userPath, request, errResp1, SC_CONFLICT);
    Exception ex =
        Assertions.assertThrows(
            UserAlreadyExistsException.class, () -> gravitinoClient.addUser(username));
    Assertions.assertEquals("user already exists", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.POST, userPath, request, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.addUser(username));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, userPath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.addUser(username), "internal error");
  }

  @Test
  public void testGetUsers() throws Exception {
    String username = "user";
    String userPath = withSlash(String.format(API_METALAKES_USERS_PATH, metalakeName, username));

    UserDTO mockUser = mockUserDTO(username);
    UserResponse userResponse = new UserResponse(mockUser);
    buildMockResource(Method.GET, userPath, null, userResponse, SC_OK);

    User loadedUser = gravitinoClient.getUser(username);
    Assertions.assertNotNull(loadedUser);
    assertUser(mockUser, loadedUser);

    // test NoSuchUserException
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchUserException.class.getSimpleName(), "user not found");
    buildMockResource(Method.GET, userPath, null, errResp1, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(NoSuchUserException.class, () -> gravitinoClient.getUser(username));
    Assertions.assertEquals("user not found", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.GET, userPath, null, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.getUser(username));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, userPath, null, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.getUser(username), "internal error");
  }

  @Test
  public void testRemoveUsers() throws Exception {
    String username = "user";
    String userPath = withSlash(String.format(API_METALAKES_USERS_PATH, metalakeName, username));

    RemoveResponse removeResponse = new RemoveResponse(true);
    buildMockResource(Method.DELETE, userPath, null, removeResponse, SC_OK);

    Assertions.assertTrue(gravitinoClient.removeUser(username));

    removeResponse = new RemoveResponse(false);
    buildMockResource(Method.DELETE, userPath, null, removeResponse, SC_OK);
    Assertions.assertFalse(gravitinoClient.removeUser(username));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, userPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(RuntimeException.class, () -> gravitinoClient.removeUser(username));
  }

  @Test
  public void testAddGroups() throws Exception {
    String groupName = "group";
    String groupPath = withSlash(String.format(API_METALAKES_GROUPS_PATH, metalakeName, ""));
    GroupAddRequest request = new GroupAddRequest(groupName);

    GroupDTO mockGroup = mockGroupDTO(groupName);
    GroupResponse groupResponse = new GroupResponse(mockGroup);
    buildMockResource(Method.POST, groupPath, request, groupResponse, SC_OK);

    Group addedGroup = gravitinoClient.addGroup(groupName);
    Assertions.assertNotNull(addedGroup);
    assertGroup(addedGroup, mockGroup);

    // test GroupAlreadyExistsException
    ErrorResponse errResp1 =
        ErrorResponse.alreadyExists(
            GroupAlreadyExistsException.class.getSimpleName(), "group already exists");
    buildMockResource(Method.POST, groupPath, request, errResp1, SC_CONFLICT);
    Exception ex =
        Assertions.assertThrows(
            GroupAlreadyExistsException.class, () -> gravitinoClient.addGroup(groupName));
    Assertions.assertEquals("group already exists", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.POST, groupPath, request, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.addGroup(groupName));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, groupPath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.addGroup(groupName), "internal error");
  }

  @Test
  public void testGetGroups() throws Exception {
    String groupName = "group";
    String groupPath = withSlash(String.format(API_METALAKES_GROUPS_PATH, metalakeName, groupName));

    GroupDTO mockGroup = mockGroupDTO(groupName);
    GroupResponse groupResponse = new GroupResponse(mockGroup);
    buildMockResource(Method.GET, groupPath, null, groupResponse, SC_OK);

    Group loadedGroup = gravitinoClient.getGroup(groupName);
    Assertions.assertNotNull(loadedGroup);
    assertGroup(mockGroup, loadedGroup);

    // test NoSuchGroupException
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchGroupException.class.getSimpleName(), "group not found");
    buildMockResource(Method.GET, groupPath, null, errResp1, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchGroupException.class, () -> gravitinoClient.getGroup(groupName));
    Assertions.assertEquals("group not found", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.GET, groupPath, null, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.getGroup(groupName));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, groupPath, null, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.getGroup(groupName), "internal error");
  }

  @Test
  public void testRemoveGroups() throws Exception {
    String groupName = "user";
    String groupPath = withSlash(String.format(API_METALAKES_GROUPS_PATH, metalakeName, groupName));

    RemoveResponse removeResponse = new RemoveResponse(true);
    buildMockResource(Method.DELETE, groupPath, null, removeResponse, SC_OK);

    Assertions.assertTrue(gravitinoClient.removeGroup(groupName));

    removeResponse = new RemoveResponse(false);
    buildMockResource(Method.DELETE, groupPath, null, removeResponse, SC_OK);
    Assertions.assertFalse(gravitinoClient.removeGroup(groupName));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, groupPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(RuntimeException.class, () -> gravitinoClient.removeGroup(groupName));
  }

  private UserDTO mockUserDTO(String name) {
    return UserDTO.builder()
        .withName(name)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private GroupDTO mockGroupDTO(String name) {
    return GroupDTO.builder()
        .withName(name)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private void assertUser(User expected, User actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.roles(), actual.roles());
  }

  private void assertGroup(Group expected, Group actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.roles(), actual.roles());
  }
}
