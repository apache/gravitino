/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static javax.servlet.http.HttpServletResponse.SC_CONFLICT;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.datastrato.gravitino.authorization.User;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.authorization.UserDTO;
import com.datastrato.gravitino.dto.requests.UserAddRequest;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.dto.responses.UserResponse;
import com.datastrato.gravitino.exceptions.UserAlreadyExistsException;
import java.time.Instant;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestMetalakeAdmin extends TestBase {

  private static final String API_ADMINS_PATH = "api/admins/%s";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
  }

  @Test
  public void testAddMetalakeAdmin() throws Exception {
    String username = "user";
    String userPath = withSlash(String.format(API_ADMINS_PATH, ""));
    UserAddRequest request = new UserAddRequest(username);

    UserDTO mockUser = mockUserDTO(username);
    UserResponse userResponse = new UserResponse(mockUser);
    buildMockResource(Method.POST, userPath, request, userResponse, SC_OK);

    User addedUser = client.addMetalakeAdmin(username);
    Assertions.assertNotNull(addedUser);
    assertUser(addedUser, mockUser);

    // test UserAlreadyExistsException
    ErrorResponse errResp1 =
        ErrorResponse.alreadyExists(
            UserAlreadyExistsException.class.getSimpleName(), "user already exists");
    buildMockResource(Method.POST, userPath, request, errResp1, SC_CONFLICT);
    Exception ex =
        Assertions.assertThrows(
            UserAlreadyExistsException.class, () -> client.addMetalakeAdmin(username));
    Assertions.assertEquals("user already exists", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, userPath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.addMetalakeAdmin(username), "internal error");
  }

  @Test
  public void testRemoveMetalakeAdmin() throws Exception {
    String username = "user";
    String rolePath = withSlash(String.format(API_ADMINS_PATH, username));

    RemoveResponse removeResponse = new RemoveResponse(true);
    buildMockResource(Method.DELETE, rolePath, null, removeResponse, SC_OK);

    Assertions.assertTrue(client.removeMetalakeAdmin(username));

    removeResponse = new RemoveResponse(false);
    buildMockResource(Method.DELETE, rolePath, null, removeResponse, SC_OK);
    Assertions.assertFalse(client.removeMetalakeAdmin(username));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, rolePath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(RuntimeException.class, () -> client.removeMetalakeAdmin(username));
  }

  private UserDTO mockUserDTO(String name) {
    return UserDTO.builder()
        .withName(name)
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private void assertUser(User expected, User actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(expected.roles(), actual.roles());
  }
}
