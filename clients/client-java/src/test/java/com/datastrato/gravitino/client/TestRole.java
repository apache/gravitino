/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.client;

import static com.datastrato.gravitino.dto.util.DTOConverters.toDTO;
import static javax.servlet.http.HttpServletResponse.SC_CONFLICT;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.hc.core5.http.HttpStatus.SC_SERVER_ERROR;

import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.dto.AuditDTO;
import com.datastrato.gravitino.dto.authorization.PrivilegeDTO;
import com.datastrato.gravitino.dto.authorization.RoleDTO;
import com.datastrato.gravitino.dto.authorization.SecurableObjectDTO;
import com.datastrato.gravitino.dto.requests.RoleCreateRequest;
import com.datastrato.gravitino.dto.responses.DeleteResponse;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.RoleResponse;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Instant;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestRole extends TestBase {

  private static final String API_METALAKES_ROLES_PATH = "api/metalakes/%s/roles/%s";
  protected static final String metalakeName = "testMetalake";

  @BeforeAll
  public static void setUp() throws Exception {
    TestBase.setUp();
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
        client.createRole(
            metalakeName,
            roleName,
            ImmutableMap.of("k1", "v1"),
            Lists.newArrayList(securableObject));
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
                client.createRole(
                    metalakeName,
                    roleName,
                    ImmutableMap.of("k1", "v1"),
                    Lists.newArrayList(securableObject)));
    Assertions.assertEquals("role already exists", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.POST, rolePath, request, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class,
            () ->
                client.createRole(
                    metalakeName,
                    roleName,
                    ImmutableMap.of("k1", "v1"),
                    Lists.newArrayList(securableObject)));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.POST, rolePath, request, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class,
        () ->
            client.createRole(
                metalakeName,
                roleName,
                ImmutableMap.of("k1", "v1"),
                Lists.newArrayList(securableObject)),
        "internal error");
  }

  @Test
  public void testGetRoles() throws Exception {
    String roleName = "role";
    String rolePath = withSlash(String.format(API_METALAKES_ROLES_PATH, metalakeName, roleName));

    RoleDTO mockRole = mockRoleDTO(roleName);
    RoleResponse roleResponse = new RoleResponse(mockRole);
    buildMockResource(Method.GET, rolePath, null, roleResponse, SC_OK);

    Role loadedRole = client.getRole(metalakeName, roleName);
    Assertions.assertNotNull(loadedRole);
    assertRole(mockRole, loadedRole);

    // test NoSuchRoleException
    ErrorResponse errResp1 =
        ErrorResponse.notFound(NoSuchRoleException.class.getSimpleName(), "role not found");
    buildMockResource(Method.GET, rolePath, null, errResp1, SC_NOT_FOUND);
    Exception ex =
        Assertions.assertThrows(
            NoSuchRoleException.class, () -> client.getRole(metalakeName, roleName));
    Assertions.assertEquals("role not found", ex.getMessage());

    // test NoSuchMetalakeException
    ErrorResponse errResp2 =
        ErrorResponse.notFound(NoSuchMetalakeException.class.getSimpleName(), "metalake not found");
    buildMockResource(Method.GET, rolePath, null, errResp2, SC_NOT_FOUND);
    ex =
        Assertions.assertThrows(
            NoSuchMetalakeException.class, () -> client.getRole(metalakeName, roleName));
    Assertions.assertEquals("metalake not found", ex.getMessage());

    // test RuntimeException
    ErrorResponse errResp3 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, rolePath, null, errResp3, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.getRole(metalakeName, roleName), "internal error");

    // test SecurableDTO use parent method
    Role testParentRole = mockHasParentRoleDTO("test");
    Assertions.assertEquals("schema", testParentRole.securableObjects().get(0).name());
    Assertions.assertEquals(
        SecurableObject.Type.SCHEMA, testParentRole.securableObjects().get(0).type());
    Assertions.assertEquals("catalog", testParentRole.securableObjects().get(0).parentFullName());
    Assertions.assertEquals("catalog", testParentRole.securableObjects().get(0).parentFullName());
  }

  @Test
  public void testDeleteRoles() throws Exception {
    String roleName = "role";
    String rolePath = withSlash(String.format(API_METALAKES_ROLES_PATH, metalakeName, roleName));

    DeleteResponse deleteResponse = new DeleteResponse(true);
    buildMockResource(Method.DELETE, rolePath, null, deleteResponse, SC_OK);

    Assertions.assertTrue(client.deleteRole(metalakeName, roleName));

    deleteResponse = new DeleteResponse(false);
    buildMockResource(Method.DELETE, rolePath, null, deleteResponse, SC_OK);
    Assertions.assertFalse(client.deleteRole(metalakeName, roleName));

    // test RuntimeException
    ErrorResponse errResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, rolePath, null, errResp, SC_SERVER_ERROR);
    Assertions.assertThrows(
        RuntimeException.class, () -> client.deleteRole(metalakeName, roleName));
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
