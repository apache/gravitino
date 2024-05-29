/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.server.web.rest;

import static com.datastrato.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static com.datastrato.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.GravitinoEnv;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.Privileges;
import com.datastrato.gravitino.authorization.Role;
import com.datastrato.gravitino.authorization.SecurableObject;
import com.datastrato.gravitino.authorization.SecurableObjects;
import com.datastrato.gravitino.catalog.CatalogDispatcher;
import com.datastrato.gravitino.dto.authorization.RoleDTO;
import com.datastrato.gravitino.dto.authorization.SecurableObjectDTO;
import com.datastrato.gravitino.dto.requests.RoleCreateRequest;
import com.datastrato.gravitino.dto.responses.DeleteResponse;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.RoleResponse;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.exceptions.NoSuchRoleException;
import com.datastrato.gravitino.exceptions.RoleAlreadyExistsException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.rest.RESTUtils;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestRoleOperations extends JerseyTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);
  private static final CatalogDispatcher dispatcher = mock(CatalogDispatcher.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  public static void setup() {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    GravitinoEnv.getInstance().setLockManager(new LockManager(config));
    GravitinoEnv.getInstance().setAccessControlManager(manager);
    GravitinoEnv.getInstance().setCatalogDispatcher(dispatcher);
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
    resourceConfig.register(RoleOperations.class);
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
  public void testCreateRole() {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));

    RoleCreateRequest req =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            new SecurableObjectDTO[] {DTOConverters.toDTO(securableObject)});
    Role role = buildRole("role1");

    when(manager.createRole(any(), any(), any(), any())).thenReturn(role);
    when(dispatcher.catalogExists(any())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    RoleResponse roleResponse = resp.readEntity(RoleResponse.class);
    Assertions.assertEquals(0, roleResponse.getCode());

    RoleDTO roleDTO = roleResponse.getRole();
    Assertions.assertEquals("role1", roleDTO.name());
    Assertions.assertEquals(
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))
            .fullName(),
        roleDTO.securableObjects().get(0).fullName());
    Assertions.assertEquals(1, roleDTO.securableObjects().get(0).privileges().size());
    Assertions.assertEquals(
        Privileges.UseCatalog.allow().name(),
        roleDTO.securableObjects().get(0).privileges().get(0).name());
    Assertions.assertEquals(
        Privileges.UseCatalog.allow().condition(),
        roleDTO.securableObjects().get(0).privileges().get(0).condition());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error"))
        .when(manager)
        .createRole(any(), any(), any(), any());
    Response resp1 =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw RoleAlreadyExistsException
    doThrow(new RoleAlreadyExistsException("mock error"))
        .when(manager)
        .createRole(any(), any(), any(), any());
    Response resp2 =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse1.getCode());
    Assertions.assertEquals(
        RoleAlreadyExistsException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(manager)
        .createRole(any(), any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  @Test
  public void testGetRole() {
    Role role = buildRole("role1");

    when(manager.getRole(any(), any())).thenReturn(role);

    Response resp =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    RoleResponse roleResponse = resp.readEntity(RoleResponse.class);
    Assertions.assertEquals(0, roleResponse.getCode());
    RoleDTO roleDTO = roleResponse.getRole();
    Assertions.assertEquals("role1", roleDTO.name());
    Assertions.assertTrue(role.properties().isEmpty());
    Assertions.assertEquals(
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))
            .fullName(),
        roleDTO.securableObjects().get(0).fullName());
    Assertions.assertEquals(1, roleDTO.securableObjects().get(0).privileges().size());
    Assertions.assertEquals(
        Privileges.UseCatalog.allow().name(),
        roleDTO.securableObjects().get(0).privileges().get(0).name());
    Assertions.assertEquals(
        Privileges.UseCatalog.allow().condition(),
        roleDTO.securableObjects().get(0).privileges().get(0).condition());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).getRole(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchRoleException
    doThrow(new NoSuchRoleException("mock error")).when(manager).getRole(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NoSuchRoleException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).getRole(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());
  }

  private Role buildRole(String role) {
    SecurableObject catalog =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    return RoleEntity.builder()
        .withId(1L)
        .withName(role)
        .withProperties(Collections.emptyMap())
        .withSecurableObject(catalog)
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  @Test
  public void testDeleteRole() {
    when(manager.deleteRole(any(), any())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    DeleteResponse deleteResponse = resp.readEntity(DeleteResponse.class);
    Assertions.assertEquals(0, deleteResponse.getCode());
    Assertions.assertTrue(deleteResponse.deleted());

    // Test when failed to delete role
    when(manager.deleteRole(any(), any())).thenReturn(false);
    Response resp2 =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    DeleteResponse deleteResponse2 = resp2.readEntity(DeleteResponse.class);
    Assertions.assertEquals(0, deleteResponse2.getCode());
    Assertions.assertFalse(deleteResponse2.deleted());

    doThrow(new RuntimeException("mock error")).when(manager).deleteRole(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());
  }
}
