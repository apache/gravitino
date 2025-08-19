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
import static org.apache.gravitino.dto.util.DTOConverters.toDTO;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.authorization.RoleDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.RoleResponse;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRole extends TestBase {

  private static final String API_METALAKES_ROLES_PATH = "api/metalakes/%s/roles/%s";
  private static final String metalakeName = "testMetalake";
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
  public void testCreateRoles() throws Exception {
    String roleName = "role";
    String rolePath = withSlash(String.format(API_METALAKES_ROLES_PATH, metalakeName, ""));
    RoleCreateRequest request =
        new RoleCreateRequest(
            roleName,
            ImmutableMap.of("k1", "v1"),
            new SecurableObjectDTO[] {
              SecurableObjectDTO.builder()
                  .withFullName("catalog")
                  .withType(SecurableObject.Type.CATALOG)
                  .withPrivileges(new PrivilegeDTO[] {toDTO(Privileges.UseCatalog.allow())})
                  .build()
            });

    RoleDTO mockRole = mockRoleDTO(roleName);
    RoleResponse roleResponse = new RoleResponse(mockRole);
    buildMockResource(Method.POST, rolePath, request, roleResponse, SC_OK);

    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));

    Role createdRole =
        gravitinoClient.createRole(
            roleName, ImmutableMap.of("k1", "v1"), Lists.newArrayList(securableObject));
    Assertions.assertEquals(1L, Privileges.CreateCatalog.allow().name().getLowBits());
    Assertions.assertEquals(0L, Privileges.CreateCatalog.allow().name().getHighBits());
    Assertions.assertNotNull(createdRole);
    assertRole(createdRole, mockRole);

    // test RoleAlreadyExistsException
    ErrorResponse errResp1 =
        ErrorResponse.alreadyExists(
            RoleAlreadyExistsException.class.getSimpleName(), "role already exists");
    buildMockResource(Method.POST, rolePath, request, errResp1, SC_CONFLICT);
    Exception ex =
        Assertions.assertThrows(
            RoleAlreadyExistsException.class,
            () ->
                gravitinoClient.createRole(
                    roleName, ImmutableMap.of("k1", "v1"), Lists.newArrayList(securableObject)));
    Assertions.assertEquals("role already exists", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.POST, rolePath, request, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                gravitinoClient.createRole(
                    roleName, ImmutableMap.of("k1", "v1"), Lists.newArrayList(securableObject)));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, rolePath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            gravitinoClient.createRole(
                roleName, ImmutableMap.of("k1", "v1"), Lists.newArrayList(securableObject)),
        "internal error");
  }

  @Test
  public void testGetRoles() throws Exception {
    String roleName = "role";
    String rolePath = withSlash(String.format(API_METALAKES_ROLES_PATH, metalakeName, roleName));

    RoleDTO mockRole = mockRoleDTO(roleName);
    RoleResponse roleResponse = new RoleResponse(mockRole);
    buildMockResource(Method.GET, rolePath, null, roleResponse, SC_OK);

    Role loadedRole = gravitinoClient.getRole(roleName);
    Assertions.assertNotNull(loadedRole);
    assertRole(mockRole, loadedRole);

    // test NoSuchRoleException
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchRoleException.class.getSimpleName(), "role not found");
    buildMockResource(Method.GET, rolePath, null, errResp1, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(NoSuchRoleException.class, () -> gravitinoClient.getRole(roleName));
    Assertions.assertEquals("role not found", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.GET, rolePath, null, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.getRole(roleName));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, rolePath, null, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> gravitinoClient.getRole(roleName), "internal error");

    // test SecurableDTO use parent method
    Role testParentRole = mockHasParentRoleDTO("test");
    Assertions.assertEquals("schema", testParentRole.securableObjects().get(0).name());
    Assertions.assertEquals(
        SecurableObject.Type.SCHEMA, testParentRole.securableObjects().get(0).type());
    Assertions.assertEquals("catalog", testParentRole.securableObjects().get(0).parent());
    Assertions.assertEquals("catalog", testParentRole.securableObjects().get(0).parent());
  }

  @Test
  public void testDeleteRoles() throws Exception {
    String roleName = "role";
    String rolePath = withSlash(String.format(API_METALAKES_ROLES_PATH, metalakeName, roleName));

    DropResponse dropResponse = new DropResponse(true, true);
    buildMockResource(Method.DELETE, rolePath, null, dropResponse, SC_OK);

    Assertions.assertTrue(gravitinoClient.deleteRole(roleName));

    dropResponse = new DropResponse(false, false);
    buildMockResource(Method.DELETE, rolePath, null, dropResponse, SC_OK);
    Assertions.assertFalse(gravitinoClient.deleteRole(roleName));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, rolePath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(RuntimeException.class, () -> gravitinoClient.deleteRole(roleName));
  }

  @Test
  public void testListRoleNames() throws Exception {
    String rolePath = withSlash(String.format(API_METALAKES_ROLES_PATH, metalakeName, ""));

    NameListResponse listResponse = new NameListResponse(new String[] {"role1", "role2"});
    buildMockResource(Method.GET, rolePath, null, listResponse, SC_OK);

    Assertions.assertArrayEquals(new String[] {"role1", "role2"}, gravitinoClient.listRoleNames());

    ErrorResponse errRespNoMetalake =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.GET, rolePath, null, errRespNoMetalake, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> gravitinoClient.listRoleNames());
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // Test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, rolePath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(RuntimeException.class, () -> gravitinoClient.listRoleNames());
  }

  private RoleDTO mockRoleDTO(String name) {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));

    return RoleDTO.builder()
        .withName(name)
        .withProperties(ImmutableMap.of("k1", "v1"))
        .withSecurableObjects(
            new SecurableObjectDTO[] {DTOConverters.toSecurableObject(securableObject)})
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private RoleDTO mockHasParentRoleDTO(String name) {
    SecurableObject schema =
        SecurableObjects.ofSchema(
            SecurableObjects.ofCatalog(
                "catalog", Lists.newArrayList(Privileges.UseCatalog.allow())),
            "schema",
            Lists.newArrayList(Privileges.UseSchema.allow()));
    return RoleDTO.builder()
        .withName(name)
        .withProperties(ImmutableMap.of("k1", "v1"))
        .withSecurableObjects(new SecurableObjectDTO[] {DTOConverters.toSecurableObject(schema)})
        .withAudit(AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  private void assertRole(Role expected, Role actual) {
    Assertions.assertEquals(expected.name(), actual.name());
    Assertions.assertEquals(
        expected.securableObjects().get(0).privileges().size(),
        actual.securableObjects().get(0).privileges().size());
    SecurableObject expectObject = expected.securableObjects().get(0);
    SecurableObject actualObject = actual.securableObjects().get(0);
    int size = expectObject.privileges().size();
    for (int index = 0; index < size; index++) {
      Assertions.assertEquals(
          expectObject.privileges().get(0).name(), actualObject.privileges().get(0).name());
      Assertions.assertEquals(
          expectObject.privileges().get(0).condition(),
          actualObject.privileges().get(0).condition());
    }
    Assertions.assertEquals(
        expected.securableObjects().get(0).fullName(), actual.securableObjects().get(0).fullName());
  }
}
