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
import com.datastrato.gravitino.auth.AuthConstants;
import com.datastrato.gravitino.authorization.AccessControlManager;
import com.datastrato.gravitino.authorization.Group;
import com.datastrato.gravitino.dto.authorization.GroupDTO;
import com.datastrato.gravitino.dto.requests.GroupAddRequest;
import com.datastrato.gravitino.dto.responses.ErrorConstants;
import com.datastrato.gravitino.dto.responses.ErrorResponse;
import com.datastrato.gravitino.dto.responses.GroupResponse;
import com.datastrato.gravitino.dto.responses.RemoveResponse;
import com.datastrato.gravitino.exceptions.ForbiddenException;
import com.datastrato.gravitino.exceptions.GroupAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchGroupException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.datastrato.gravitino.lock.LockManager;
import com.datastrato.gravitino.meta.AuditInfo;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.metalake.MetalakeManager;
import com.datastrato.gravitino.rest.RESTUtils;
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

public class TestGroupOperations extends JerseyTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);
  private static final MetalakeManager metalakeManager = mock(MetalakeManager.class);
  private static final BaseMetalake metalake = mock(BaseMetalake.class);

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
    resourceConfig.register(GroupOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
            bind(metalakeManager).to(MetalakeManager.class).ranked(2);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testAddGroup() {
    GroupAddRequest req = new GroupAddRequest("group1");
    Group group = buildGroup("group1");

    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(AuthConstants.ANONYMOUS_USER)
            .withCreateTime(Instant.now())
            .build();
    when(manager.isMetalakeAdmin(any())).thenReturn(true);
    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);
    when(metalake.auditInfo()).thenReturn(auditInfo);
    when(manager.addGroup(any(), any())).thenReturn(group);

    Response resp =
        target("/metalakes/metalake1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    GroupResponse groupResponse = resp.readEntity(GroupResponse.class);
    Assertions.assertEquals(0, groupResponse.getCode());

    GroupDTO groupDTO = groupResponse.getGroup();
    Assertions.assertEquals("group1", groupDTO.name());
    Assertions.assertNotNull(groupDTO.roles());
    Assertions.assertTrue(groupDTO.roles().isEmpty());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).addGroup(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp1.getMediaType());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw GroupAlreadyExistsException
    doThrow(new GroupAlreadyExistsException("mock error")).when(manager).addGroup(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResponse1.getCode());
    Assertions.assertEquals(
        GroupAlreadyExistsException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).addGroup(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());

    // Test with forbidden request
    when(manager.isMetalakeAdmin(any())).thenReturn(false);
    Response resp4 =
        target("/metalakes/metalake1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResponse4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, errorResponse4.getCode());
    Assertions.assertEquals(ForbiddenException.class.getSimpleName(), errorResponse4.getType());

    when(manager.isMetalakeAdmin(any())).thenReturn(true);
    auditInfo = AuditInfo.builder().withCreator("user").withCreateTime(Instant.now()).build();
    when(metalake.auditInfo()).thenReturn(auditInfo);

    Response resp5 =
        target("/metalakes/metalake1/groups")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResponse5 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, errorResponse5.getCode());
    Assertions.assertEquals(ForbiddenException.class.getSimpleName(), errorResponse5.getType());
  }

  @Test
  public void testGetGroup() {
    Group group = buildGroup("group1");

    when(manager.getGroup(any(), any())).thenReturn(group);
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(AuthConstants.ANONYMOUS_USER)
            .withCreateTime(Instant.now())
            .build();
    when(manager.isMetalakeAdmin(any())).thenReturn(true);
    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);
    when(metalake.auditInfo()).thenReturn(auditInfo);

    Response resp =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    GroupResponse groupResponse = resp.readEntity(GroupResponse.class);
    Assertions.assertEquals(0, groupResponse.getCode());
    GroupDTO groupDTO = groupResponse.getGroup();
    Assertions.assertEquals("group1", groupDTO.name());
    Assertions.assertNotNull(groupDTO.roles());
    Assertions.assertTrue(groupDTO.roles().isEmpty());

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).getGroup(any(), any());
    Response resp1 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw NoSuchGroupException
    doThrow(new NoSuchGroupException("mock error")).when(manager).getGroup(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NoSuchGroupException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).getGroup(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse2.getType());

    // Test with forbidden request
    when(manager.isMetalakeAdmin(any())).thenReturn(false);
    Response resp4 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResponse4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, errorResponse4.getCode());
    Assertions.assertEquals(ForbiddenException.class.getSimpleName(), errorResponse4.getType());

    when(manager.isMetalakeAdmin(any())).thenReturn(true);
    auditInfo = AuditInfo.builder().withCreator("user").withCreateTime(Instant.now()).build();
    when(metalake.auditInfo()).thenReturn(auditInfo);

    Response resp5 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResponse5 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, errorResponse5.getCode());
    Assertions.assertEquals(ForbiddenException.class.getSimpleName(), errorResponse5.getType());
  }

  private Group buildGroup(String group) {
    return GroupEntity.builder()
        .withId(1L)
        .withName(group)
        .withRoles(Collections.emptyList())
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }

  @Test
  public void testRemoveGroup() {
    when(manager.removeGroup(any(), any())).thenReturn(true);
    AuditInfo auditInfo =
        AuditInfo.builder()
            .withCreator(AuthConstants.ANONYMOUS_USER)
            .withCreateTime(Instant.now())
            .build();
    when(manager.isMetalakeAdmin(any())).thenReturn(true);
    when(metalakeManager.loadMetalake(any())).thenReturn(metalake);
    when(metalake.auditInfo()).thenReturn(auditInfo);

    Response resp =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    RemoveResponse removeResponse = resp.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse.getCode());
    Assertions.assertTrue(removeResponse.removed());

    // Test when failed to remove group
    when(manager.removeGroup(any(), any())).thenReturn(false);
    Response resp2 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    RemoveResponse removeResponse2 = resp2.readEntity(RemoveResponse.class);
    Assertions.assertEquals(0, removeResponse2.getCode());
    Assertions.assertFalse(removeResponse2.removed());

    doThrow(new RuntimeException("mock error")).when(manager).removeGroup(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResponse.getType());

    // Test with forbidden request
    when(manager.isMetalakeAdmin(any())).thenReturn(false);
    Response resp4 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp4.getStatus());

    ErrorResponse errorResponse4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, errorResponse4.getCode());
    Assertions.assertEquals(ForbiddenException.class.getSimpleName(), errorResponse4.getType());

    when(manager.isMetalakeAdmin(any())).thenReturn(true);
    auditInfo = AuditInfo.builder().withCreator("user").withCreateTime(Instant.now()).build();
    when(metalake.auditInfo()).thenReturn(auditInfo);

    Response resp5 =
        target("/metalakes/metalake1/groups/group1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp5.getStatus());

    ErrorResponse errorResponse5 = resp5.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.FORBIDDEN_CODE, errorResponse5.getCode());
    Assertions.assertEquals(ForbiddenException.class.getSimpleName(), errorResponse5.getType());
  }
}
