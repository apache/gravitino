/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.datastrato.gravitino.dto.requests.RoleGrantRequest;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.GrantResponse;
import com.datastrato.gravitino.dto.responses.RevokeResponse;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestPermission extends TestBase {

  private static final String metalakeName = "testMetalake";
  private static final String API_PERMISSION_PATH = "/api/metalakes/%s/permissions/%s";
  private static final String PERMISSION_USER_PATH = "users/%s/roles/%s";
  private static final String PERMISSION_GROUP_PATH = "groups/%s/roles/%s";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
  }

  @Test
  public void testGrantRoleToUser() throws Exception {
    String role = "role";
    String user = "user";
    String userPath =
        String.format(
            API_PERMISSION_PATH, metalakeName, String.format(PERMISSION_USER_PATH, user, ""));
    RoleGrantRequest request = new RoleGrantRequest(role);
    GrantResponse response = new GrantResponse(true);

    buildMockResource(Method.POST, userPath, request, response, SC_OK);
    Assertions.assertTrue(client.grantRoleToUser(metalakeName, role, user));

    // test Exception
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, userPath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertFalse(client.grantRoleToUser(metalakeName, role, user));
  }

  @Test
  public void testRevokeRoleFromUser() throws Exception {
    String role = "role";
    String user = "user";
    String userPath =
        String.format(
            API_PERMISSION_PATH, metalakeName, String.format(PERMISSION_USER_PATH, user, role));
    RevokeResponse response = new RevokeResponse(true);

    buildMockResource(Method.DELETE, userPath, null, response, SC_OK);
    Assertions.assertTrue(client.revokeRoleFromUser(metalakeName, role, user));

    // test Exception
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, userPath, null, errResp3, SC_SERVER_ERROR);
    Assertions.assertFalse(client.revokeRoleFromUser(metalakeName, role, user));
  }

  @Test
  public void testGrantRoleToGroup() throws Exception {
    String role = "role";
    String group = "group";
    String groupPath =
        String.format(
            API_PERMISSION_PATH, metalakeName, String.format(PERMISSION_GROUP_PATH, group, ""));
    RoleGrantRequest request = new RoleGrantRequest(role);
    GrantResponse response = new GrantResponse(true);

    buildMockResource(Method.POST, groupPath, request, response, SC_OK);
    Assertions.assertTrue(client.grantRoleToGroup(metalakeName, role, group));

    // test Exception
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, groupPath, request, errResp, SC_SERVER_ERROR);
    Assertions.assertFalse(client.grantRoleToGroup(metalakeName, role, group));
  }

  @Test
  public void testRevokeRoleFromGroup() throws Exception {
    String role = "role";
    String group = "group";
    String groupPath =
        String.format(
            API_PERMISSION_PATH, metalakeName, String.format(PERMISSION_GROUP_PATH, group, role));
    RevokeResponse response = new RevokeResponse(true);

    buildMockResource(Method.DELETE, groupPath, null, response, SC_OK);
    Assertions.assertTrue(client.revokeRoleFromGroup(metalakeName, role, group));

    // test Exception
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, groupPath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertFalse(client.revokeRoleFromGroup(metalakeName, role, group));
  }
}
