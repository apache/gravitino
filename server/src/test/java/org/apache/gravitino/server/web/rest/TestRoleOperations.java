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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.AccessControlManager;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.catalog.TopicDispatcher;
import org.apache.gravitino.dto.authorization.PrivilegeDTO;
import org.apache.gravitino.dto.authorization.RoleDTO;
import org.apache.gravitino.dto.authorization.SecurableObjectDTO;
import org.apache.gravitino.dto.requests.RoleCreateRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.RoleResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.exceptions.IllegalNamespaceException;
import org.apache.gravitino.exceptions.IllegalPrivilegeException;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchMetadataObjectException;
import org.apache.gravitino.exceptions.NoSuchMetalakeException;
import org.apache.gravitino.exceptions.NoSuchRoleException;
import org.apache.gravitino.exceptions.RoleAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.RoleEntity;
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

public class TestRoleOperations extends BaseOperationsTest {

  private static final AccessControlManager manager = mock(AccessControlManager.class);
  private static final MetalakeDispatcher metalakeDispatcher = mock(MetalakeDispatcher.class);
  private static final CatalogDispatcher catalogDispatcher = mock(CatalogDispatcher.class);
  private static final SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
  private static final TableDispatcher tableDispatcher = mock(TableDispatcher.class);
  private static final TopicDispatcher topicDispatcher = mock(TopicDispatcher.class);
  private static final FilesetDispatcher filesetDispatcher = mock(FilesetDispatcher.class);

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
    FieldUtils.writeField(GravitinoEnv.getInstance(), "accessControlDispatcher", manager, true);
    FieldUtils.writeField(
        GravitinoEnv.getInstance(), "metalakeDispatcher", metalakeDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "catalogDispatcher", catalogDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "schemaDispatcher", schemaDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "tableDispatcher", tableDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "topicDispatcher", topicDispatcher, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "filesetDispatcher", filesetDispatcher, true);
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
  public void testCreateRole() throws IllegalAccessException, NoSuchFieldException {
    SecurableObject securableObject =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    SecurableObject anotherSecurableObject =
        SecurableObjects.ofCatalog(
            "another_catalog", Lists.newArrayList(Privileges.CreateSchema.deny()));

    RoleCreateRequest req =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            new SecurableObjectDTO[] {
              DTOConverters.toDTO(securableObject), DTOConverters.toDTO(anotherSecurableObject)
            });
    Role role = buildRole("role1");

    when(manager.createRole(any(), any(), any(), any())).thenReturn(role);
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);

    // Test with IllegalRequest
    RoleCreateRequest illegalRequest = new RoleCreateRequest("role", Collections.emptyMap(), null);
    Response illegalResp =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(illegalRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

    SecurableObjectDTO illegalObject =
        DTOConverters.toDTO(
            SecurableObjects.ofCatalog(
                "illegal_catalog", Lists.newArrayList(Privileges.CreateSchema.deny())));
    Field field = illegalObject.getClass().getDeclaredField("privileges");
    field.setAccessible(true);
    field.set(illegalObject, new PrivilegeDTO[] {});

    illegalRequest =
        new RoleCreateRequest(
            "role", Collections.emptyMap(), new SecurableObjectDTO[] {illegalObject});
    illegalResp =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(illegalRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), illegalResp.getStatus());

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
        SecurableObjects.ofCatalog(
                "another_catalog", Lists.newArrayList(Privileges.CreateSchema.deny()))
            .fullName(),
        roleDTO.securableObjects().get(1).fullName());
    Assertions.assertEquals(1, roleDTO.securableObjects().get(1).privileges().size());
    Assertions.assertEquals(
        Privileges.CreateSchema.deny().name(),
        roleDTO.securableObjects().get(1).privileges().get(0).name());
    Assertions.assertEquals(
        Privileges.UseCatalog.deny().condition(),
        roleDTO.securableObjects().get(1).privileges().get(0).condition());

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

    // Test with a wrong metalake name
    RoleCreateRequest reqWithWrongMetalake =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            new SecurableObjectDTO[] {
              DTOConverters.toDTO(
                  SecurableObjects.ofMetalake(
                      "unknown", Lists.newArrayList(Privileges.UseCatalog.allow()))),
            });
    Response respWithWrongMetalake =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(reqWithWrongMetalake, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), respWithWrongMetalake.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, respWithWrongMetalake.getMediaType());
    ErrorResponse withWrongMetalakeResponse = respWithWrongMetalake.readEntity(ErrorResponse.class);
    Assertions.assertEquals(
        ErrorConstants.ILLEGAL_ARGUMENTS_CODE, withWrongMetalakeResponse.getCode());

    // Test to a catalog which doesn't exist
    reset(catalogDispatcher);
    when(catalogDispatcher.loadCatalog(any())).thenThrow(new NoSuchCatalogException("mock error"));
    Response respNotExist =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), respNotExist.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, respNotExist.getMediaType());
    ErrorResponse notExistResponse = respNotExist.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, notExistResponse.getCode());

    // Test to throw NoSuchMetalakeException
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);
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

    // Test with wrong binding privileges
    SecurableObject wrongPrivilegeObject =
        SecurableObjects.ofCatalog("wrong", Lists.newArrayList(Privileges.CreateCatalog.allow()));
    RoleCreateRequest wrongPriRequest =
        new RoleCreateRequest(
            "role",
            Collections.emptyMap(),
            new SecurableObjectDTO[] {DTOConverters.toDTO(wrongPrivilegeObject)});

    Response wrongPrivilegeResp =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(wrongPriRequest, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.BAD_REQUEST.getStatusCode(), wrongPrivilegeResp.getStatus());

    ErrorResponse wrongPriErrorResp = wrongPrivilegeResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, wrongPriErrorResp.getCode());
    Assertions.assertEquals(
        IllegalPrivilegeException.class.getSimpleName(), wrongPriErrorResp.getType());

    // Test with empty securable objects request
    RoleCreateRequest emptyObjectRequest =
        new RoleCreateRequest("role", Collections.emptyMap(), new SecurableObjectDTO[] {});

    Role emptyObjectRole =
        RoleEntity.builder()
            .withId(1L)
            .withName("empty")
            .withProperties(Collections.emptyMap())
            .withSecurableObjects(Collections.emptyList())
            .withAuditInfo(
                AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
            .build();
    reset(manager);
    when(manager.createRole(any(), any(), any(), any())).thenReturn(emptyObjectRole);

    Response emptyObjectResp =
        target("/metalakes/metalake1/roles")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(emptyObjectRequest, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), emptyObjectResp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, emptyObjectResp.getMediaType());

    RoleResponse emptyObjectResponse = emptyObjectResp.readEntity(RoleResponse.class);
    Assertions.assertEquals(0, emptyObjectResponse.getCode());
    Role emptyRoleDTO = emptyObjectResponse.getRole();
    Assertions.assertEquals(emptyRoleDTO.name(), "empty");
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

  @Test
  public void testDeleteRole() {
    when(manager.deleteRole(any(), any())).thenReturn(true);

    Response resp =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    // Test when failed to delete role
    when(manager.deleteRole(any(), any())).thenReturn(false);
    Response resp2 =
        target("/metalakes/metalake1/roles/role1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp2.getStatus());
    DropResponse dropResponse2 = resp2.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse2.getCode());
    Assertions.assertFalse(dropResponse2.dropped());

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

  @Test
  public void testCheckSecurableObjects() {
    // check the catalog
    SecurableObject catalog =
        SecurableObjects.ofCatalog("catalog", Lists.newArrayList(Privileges.UseCatalog.allow()));
    when(catalogDispatcher.catalogExists(any())).thenReturn(true);
    Assertions.assertDoesNotThrow(
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(catalog)));
    when(catalogDispatcher.catalogExists(any())).thenReturn(false);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(catalog)));

    // check the schema
    SecurableObject schema =
        SecurableObjects.ofSchema(
            catalog, "schema", Lists.newArrayList(Privileges.UseSchema.allow()));
    when(schemaDispatcher.schemaExists(any())).thenReturn(true);
    Assertions.assertDoesNotThrow(
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(schema)));
    when(schemaDispatcher.schemaExists(any())).thenReturn(false);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(schema)));

    // check the table
    SecurableObject table =
        SecurableObjects.ofTable(
            schema, "table", Lists.newArrayList(Privileges.SelectTable.allow()));
    when(tableDispatcher.tableExists(any())).thenReturn(true);
    Assertions.assertDoesNotThrow(
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(table)));
    when(tableDispatcher.tableExists(any())).thenReturn(false);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(table)));

    // check the topic
    SecurableObject topic =
        SecurableObjects.ofTopic(
            schema, "topic", Lists.newArrayList(Privileges.ConsumeTopic.allow()));
    when(topicDispatcher.topicExists(any())).thenReturn(true);
    Assertions.assertDoesNotThrow(
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(topic)));
    when(topicDispatcher.topicExists(any())).thenReturn(false);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(topic)));

    // check the fileset
    SecurableObject fileset =
        SecurableObjects.ofFileset(
            schema, "fileset", Lists.newArrayList(Privileges.ReadFileset.allow()));
    when(filesetDispatcher.filesetExists(any())).thenReturn(true);
    Assertions.assertDoesNotThrow(
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(fileset)));
    when(filesetDispatcher.filesetExists(any())).thenReturn(false);
    Assertions.assertThrows(
        NoSuchMetadataObjectException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", DTOConverters.toDTO(fileset)));

    AtomicReference<String> wrongParent = new AtomicReference<>();
    AtomicReference<String> wrongName = new AtomicReference<>();
    AtomicReference<MetadataObject.Type> wrongType = new AtomicReference<>();
    wrongParent.set("catalog1");
    wrongName.set("schema1");

    MetadataObject wrongMetadataObject =
        new MetadataObject() {
          @Nullable
          @Override
          public String parent() {
            return wrongParent.get();
          }

          @Override
          public String name() {
            return wrongName.get();
          }

          @Override
          public Type type() {
            return wrongType.get();
          }
        };

    // Test catalog object
    wrongType.set(MetadataObject.Type.CATALOG);
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", wrongMetadataObject));

    // Test schema object
    wrongType.set(MetadataObject.Type.CATALOG);
    wrongParent.set("catalog1.schema1");
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", wrongMetadataObject));

    // Test table object
    wrongType.set(MetadataObject.Type.TABLE);
    wrongParent.set("catalog1");
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", wrongMetadataObject));

    // Test fileset object
    wrongType.set(MetadataObject.Type.FILESET);
    wrongParent.set("catalog1");
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", wrongMetadataObject));

    // Test topic object
    wrongType.set(MetadataObject.Type.TOPIC);
    wrongParent.set("catalog1");
    Assertions.assertThrows(
        IllegalNamespaceException.class,
        () -> MetadataObjectUtil.checkMetadataObject("metalake", wrongMetadataObject));
  }

  @Test
  public void testListRoleNames() {
    when(manager.listRoleNames(any())).thenReturn(new String[] {"role"});

    Response resp =
        target("/metalakes/metalake1/roles/")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    NameListResponse listResponse = resp.readEntity(NameListResponse.class);
    Assertions.assertEquals(0, listResponse.getCode());

    Assertions.assertEquals(1, listResponse.getNames().length);
    Assertions.assertEquals("role", listResponse.getNames()[0]);

    // Test to throw NoSuchMetalakeException
    doThrow(new NoSuchMetalakeException("mock error")).when(manager).listRoleNames(any());
    Response resp1 =
        target("/metalakes/metalake1/roles/")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResponse = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResponse.getCode());
    Assertions.assertEquals(NoSuchMetalakeException.class.getSimpleName(), errorResponse.getType());

    // Test to throw internal RuntimeException
    doThrow(new RuntimeException("mock error")).when(manager).listRoleNames(any());
    Response resp3 =
        target("/metalakes/metalake1/roles")
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
    SecurableObject anotherSecurableObject =
        SecurableObjects.ofCatalog(
            "another_catalog", Lists.newArrayList(Privileges.CreateSchema.deny()));

    return RoleEntity.builder()
        .withId(1L)
        .withName(role)
        .withProperties(Collections.emptyMap())
        .withSecurableObjects(Lists.newArrayList(catalog, anotherSecurableObject))
        .withAuditInfo(
            AuditInfo.builder().withCreator("creator").withCreateTime(Instant.now()).build())
        .build();
  }
}
