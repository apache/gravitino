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
package org.apache.gravitino.server.web.rest;

import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.AccessControlDispatcher;
import org.apache.gravitino.authorization.Owner;
import org.apache.gravitino.authorization.OwnerManager;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.dto.authorization.OwnerDTO;
import org.apache.gravitino.dto.requests.OwnerSetRequest;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.OwnerResponse;
import org.apache.gravitino.dto.responses.SetResponse;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.NotFoundException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.utils.MetadataObjectUtil;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestOwnerOperations extends BaseOperationsTest {
  private static final OwnerManager manager = mock(OwnerManager.class);
  private static final MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);
  private static final AccessControlDispatcher accessControlDispatcher =
      mock(AccessControlDispatcher.class);

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "ownerDispatcher", manager, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "metalakeDispatcher", metalakeDispatcher, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "accessControlDispatcher", accessControlDispatcher, true);
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
    resourceConfig.register(OwnerOperations.class);
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
  void testGetOwnerForObject() {
    Owner owner =
        new Owner() {
          @Override
          public String name() {
            return "test";
          }

          @Override
          public Type type() {
            return Type.USER;
          }
        };

    when(manager.getOwner(any(), any())).thenReturn(Optional.of(owner));
    when(metalakeDispatcher.metalakeExists(any())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    OwnerResponse ownerResponse = resp.readEntity(OwnerResponse.class);
    Assertions.assertEquals(0, ownerResponse.getCode());
    OwnerDTO ownerDTO = ownerResponse.getOwner();
    Assertions.assertEquals("test", ownerDTO.name());
    Assertions.assertEquals(Owner.Type.USER, ownerDTO.type());

    // Owner is not set
    when(manager.getOwner(any(), any())).thenReturn(Optional.empty());
    Response resp1 =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    OwnerResponse ownerResponse1 = resp1.readEntity(OwnerResponse.class);
    Assertions.assertEquals(0, ownerResponse1.getCode());
    Assertions.assertNull(ownerResponse1.getOwner());

    // Test to throw NotFoundException
    doThrow(new NotFoundException("mock error")).when(manager).getOwner(any(), any());
    Response resp2 =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NotFoundException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).getOwner(any(), any());
    Response resp3 =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());

    // Test to throw IllegalNamespaceException
    Response resp4 =
        target("/metalakes/metalake1/owners/catalog/metalake1.catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    ErrorResponse errorResponse3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse3.getCode());
  }

  @Test
  void testSetOwnerForObject() {
    when(metalakeDispatcher.metalakeExists(any())).thenReturn(true);
    OwnerSetRequest invalidRequest = new OwnerSetRequest(null, Owner.Type.USER);
    Response invalidResp =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(invalidRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), invalidResp.getStatus());

    OwnerSetRequest request = new OwnerSetRequest("test", Owner.Type.USER);
    Response resp =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    SetResponse setResponse = resp.readEntity(SetResponse.class);
    Assertions.assertEquals(0, setResponse.getCode());
    Assertions.assertTrue(setResponse.set());

    // Test to throw NotFoundException
    doThrow(new NotFoundException("mock error")).when(manager).setOwner(any(), any(), any(), any());
    Response resp2 =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResponse1 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse1.getCode());
    Assertions.assertEquals(NotFoundException.class.getSimpleName(), errorResponse1.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).setOwner(any(), any(), any(), any());
    Response resp3 =
        target("/metalakes/metalake1/owners/metalake/metalake1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResponse2 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResponse2.getCode());

    // Test to throw IllegalNamespaceException
    Response resp4 =
        target("/metalakes/metalake1/owners/catalog/metalake1.catalog1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
    ErrorResponse errorResponse3 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResponse3.getCode());
  }

  @Test
  public void testRoleObject() {
    MetadataObject role = MetadataObjects.of(null, "role", MetadataObject.Type.ROLE);
    when(accessControlDispatcher.getRole(any(), any())).thenReturn(mock(Role.class));
    Assertions.assertDoesNotThrow(() -> MetadataObjectUtil.checkMetadataObject("metalake", role));

    doThrow(new NoSuchRoleException("test")).when(accessControlDispatcher).getRole(any(), any());
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", role));
  }
}
