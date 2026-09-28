/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.client.integration.test.authorization;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.client.ErrorHandlers;
import org.apache.gravitino.client.GravitinoAdminClient;
import org.apache.gravitino.client.GravitinoMetalake;
import org.apache.gravitino.client.RESTClient;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.BulkRemoveRequest;
import org.apache.gravitino.dto.requests.BulkRoleAddRequest;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.responses.BulkRemoveResponse;
import org.apache.gravitino.dto.responses.BulkRoleResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.exceptions.ForbiddenException;
import org.apache.gravitino.rest.RESTUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Tag("gravitino-docker-test")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RoleAuthorizationIT extends BaseRestApiAuthorizationIT {

  @Test
  @Order(1)
  public void testCreateRole() {
    client.loadMetalake(METALAKE).createRole("role1", new HashMap<>(), Collections.emptyList());
    client.loadMetalake(METALAKE).createRole("role2", new HashMap<>(), Collections.emptyList());
    client.loadMetalake(METALAKE).createRole("role3", new HashMap<>(), Collections.emptyList());
    // normal user can not create role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .createRole("role4", new HashMap<>(), Collections.emptyList());
        });
    client.loadMetalake(METALAKE).grantRolesToUser(ImmutableList.of("role1"), NORMAL_USER);
    client
        .loadMetalake(METALAKE)
        .grantPrivilegesToRole(
            "role1",
            MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
            ImmutableList.of(Privileges.CreateRole.allow()));
    // normal user can create role after grant
    normalUserClient
        .loadMetalake(METALAKE)
        .createRole("role4", new HashMap<>(), Collections.emptyList());
  }

  @Test
  @Order(2)
  public void testListRole() {
    String[] roleNames = client.loadMetalake(METALAKE).listRoleNames();
    Arrays.sort(roleNames);
    assertArrayEquals(new String[] {"role1", "role2", "role3", "role4"}, roleNames);
    roleNames = normalUserClient.loadMetalake(METALAKE).listRoleNames();
    Arrays.sort(roleNames);
    assertArrayEquals(new String[] {"role1", "role4"}, roleNames);
    client
        .loadMetalake(METALAKE)
        .grantPrivilegesToRole(
            "role1",
            MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
            ImmutableSet.of(Privileges.ManageGrants.allow()));
    roleNames = normalUserClient.loadMetalake(METALAKE).listRoleNames();
    assertArrayEquals(new String[] {"role1", "role2", "role3", "role4"}, roleNames);
    client
        .loadMetalake(METALAKE)
        .revokePrivilegesFromRole(
            "role1",
            MetadataObjects.of(ImmutableList.of(METALAKE), MetadataObject.Type.METALAKE),
            ImmutableSet.of(Privileges.ManageGrants.allow()));
  }

  @Test
  @Order(3)
  public void testGetRole() {
    client.loadMetalake(METALAKE).getRole("role1");
    client.loadMetalake(METALAKE).getRole("role2");
    client.loadMetalake(METALAKE).getRole("role3");
    client.loadMetalake(METALAKE).getRole("role4");
    normalUserClient.loadMetalake(METALAKE).getRole("role1");
    // normal user can not get role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).getRole("role2");
        });
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).getRole("role3");
        });
  }

  @Test
  @Order(4)
  public void testDeleteRole() {
    // normal user can not delete role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).deleteRole("role1");
        });
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).deleteRole("role2");
        });
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient.loadMetalake(METALAKE).deleteRole("role3");
        });
    // owner can delete role
    client.loadMetalake(METALAKE).deleteRole("role1");
    client.loadMetalake(METALAKE).deleteRole("role2");
    client.loadMetalake(METALAKE).deleteRole("role3");
    // normal user can not create role after delete role
    assertThrows(
        "Current user can not create role.",
        ForbiddenException.class,
        () -> {
          normalUserClient
              .loadMetalake(METALAKE)
              .createRole("role2", new HashMap<>(), Collections.emptyList());
        });
  }

  @Test
  @Order(5)
  public void testBulkRoleInterfaces() throws Exception {
    String bulkRoleManager = "bulk_role_manager";
    String bulkRole1 = "bulk_role_it_1";
    String bulkRole2 = "bulk_role_it_2";
    String adminRole = "bulk_role_admin";
    GravitinoMetalake gravitinoMetalake = client.loadMetalake(METALAKE);
    gravitinoMetalake.addUser(bulkRoleManager);
    gravitinoMetalake.createRole(adminRole, new HashMap<>(), Collections.emptyList());

    assertThrows(
        ForbiddenException.class,
        () ->
            bulkAddRoles(
                restClient(normalUserClient),
                new BulkRoleAddRequest(
                    new RoleCreateRequest[] {
                      new RoleCreateRequest(bulkRole1, new HashMap<>(), new SecurableObjectDTO[] {})
                    })));

    BulkRemoveResponse unauthorizedRemoveResponse =
        bulkRemoveRoles(
            restClient(normalUserClient), new BulkRemoveRequest(new String[] {adminRole}));
    assertEquals(0, unauthorizedRemoveResponse.getNames().length);
    assertEquals(1, unauthorizedRemoveResponse.getErrors().length);
    assertEquals(
        ErrorConstants.FORBIDDEN_CODE, unauthorizedRemoveResponse.getErrors()[0].getCode());

    gravitinoMetalake.createRole(
        "bulk_role_create_grant", new HashMap<>(), Collections.emptyList());
    gravitinoMetalake.grantPrivilegesToRole(
        "bulk_role_create_grant",
        MetadataObjects.of(null, METALAKE, MetadataObject.Type.METALAKE),
        ImmutableList.of(Privileges.CreateRole.allow()));
    gravitinoMetalake.grantRolesToUser(ImmutableList.of("bulk_role_create_grant"), bulkRoleManager);

    GravitinoAdminClient bulkRoleManagerClient = getClientByUser(bulkRoleManager);
    BulkRoleResponse addResponse =
        bulkAddRoles(
            restClient(bulkRoleManagerClient),
            new BulkRoleAddRequest(
                new RoleCreateRequest[] {
                  new RoleCreateRequest(bulkRole1, new HashMap<>(), new SecurableObjectDTO[] {}),
                  new RoleCreateRequest(bulkRole2, new HashMap<>(), new SecurableObjectDTO[] {})
                }));
    assertEquals(2, addResponse.getRoles().length);
    assertEquals(0, addResponse.getErrors().length);

    BulkRemoveResponse removeResponse =
        bulkRemoveRoles(
            restClient(bulkRoleManagerClient),
            new BulkRemoveRequest(new String[] {bulkRole1, bulkRole2}));
    assertArrayEquals(new String[] {bulkRole1, bulkRole2}, removeResponse.getNames());
    assertEquals(0, removeResponse.getErrors().length);

    gravitinoMetalake.deleteRole(adminRole);
    gravitinoMetalake.removeUser(bulkRoleManager);
  }

  @Test
  @Order(6)
  public void testListRolesWithNonExistentMetalake() throws Exception {
    // Test that listRoles with @AuthorizationExpression returns 403 Forbidden
    // when the metalake doesn't exist, instead of 404 response
    String nonExistentMetalake = "nonExistentMetalake";

    // Access the restClient from normalUserClient using reflection
    Method restClientMethod =
        normalUserClient.getClass().getSuperclass().getDeclaredMethod("restClient");
    restClientMethod.setAccessible(true);
    Object restClient = restClientMethod.invoke(normalUserClient);

    // Create a MetalakeDTO for the non-existent metalake
    MetalakeDTO metalakeDTO =
        MetalakeDTO.builder()
            .withName(nonExistentMetalake)
            .withComment("test")
            .withProperties(Maps.newHashMap())
            .withAudit(
                org.apache.gravitino.dto.AuditDTO.builder()
                    .withCreator("test")
                    .withCreateTime(java.time.Instant.now())
                    .build())
            .build();

    // Use DTOConverters.toMetaLake() via reflection to create GravitinoMetalake
    Class<?> dtoConvertersClass = Class.forName("org.apache.gravitino.client.DTOConverters");
    Method toMetaLakeMethod =
        dtoConvertersClass.getDeclaredMethod(
            "toMetaLake",
            MetalakeDTO.class,
            Class.forName("org.apache.gravitino.client.RESTClient"));
    toMetaLakeMethod.setAccessible(true);
    GravitinoMetalake nonExistentMetalakeObj =
        (GravitinoMetalake) toMetaLakeMethod.invoke(null, metalakeDTO, restClient);

    // Test listRoleNames - should return 403 ForbiddenException
    assertThrows(ForbiddenException.class, nonExistentMetalakeObj::listRoleNames);
  }

  private GravitinoAdminClient getClientByUser(String username) {
    GravitinoAdminClient client =
        GravitinoAdminClient.builder(serverUri).withSimpleAuth(username).build();
    closer.register(client);
    return client;
  }

  private BulkRoleResponse bulkAddRoles(RESTClient restClient, BulkRoleAddRequest request) {
    BulkRoleResponse response =
        restClient.post(
            String.format("api/bulk/metalakes/%s/roles/add", RESTUtils.encodeString(METALAKE)),
            request,
            BulkRoleResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    response.validate();
    return response;
  }

  private BulkRemoveResponse bulkRemoveRoles(RESTClient restClient, BulkRemoveRequest request) {
    BulkRemoveResponse response =
        restClient.post(
            String.format("api/bulk/metalakes/%s/roles/remove", RESTUtils.encodeString(METALAKE)),
            request,
            BulkRemoveResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.roleErrorHandler());
    response.validate();
    return response;
  }
}
