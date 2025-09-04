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
import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Config;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Version;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.audit.FilesetAuditConstants;
import org.apache.gravitino.audit.FilesetDataOperation;
import org.apache.gravitino.audit.InternalClientType;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.catalog.FilesetOperationDispatcher;
import org.apache.gravitino.dto.file.FilesetDTO;
import org.apache.gravitino.dto.requests.FilesetCreateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.FileLocationResponse;
import org.apache.gravitino.dto.responses.FilesetResponse;
import org.apache.gravitino.exceptions.FilesetAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.exceptions.NoSuchLocationNameException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

public class TestFilesetOperations extends BaseOperationsTest {
  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      when(request.getHeader(Version.CLIENT_VERSION_HEADER)).thenReturn(null);
      return request;
    }
  }

  private FilesetOperationDispatcher dispatcher = mock(FilesetOperationDispatcher.class);

  private final String metalake = "metalake1";

  private final String catalog = "catalog1";

  private final String schema = "schema1";

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
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
    resourceConfig.register(FilesetOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(FilesetDispatcher.class).ranked(2);
            bindFactory(MockServletRequestFactory.class).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListFileset() {
    NameIdentifier fileset1 = NameIdentifier.of(metalake, catalog, schema, "fileset1");
    NameIdentifier fileset2 = NameIdentifier.of(metalake, catalog, schema, "fileset2");

    when(dispatcher.listFilesets(any())).thenReturn(new NameIdentifier[] {fileset1, fileset2});
    Response resp =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    EntityListResponse listResp = resp.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    NameIdentifier[] filesets = listResp.identifiers();
    Assertions.assertEquals(2, filesets.length);
    Assertions.assertEquals(fileset1, filesets[0]);
    Assertions.assertEquals(fileset2, filesets[1]);

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(dispatcher).listFilesets(any());
    Response resp1 =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).listFilesets(any());
    Response resp2 =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void loadFileset() {
    Fileset fileset =
        mockFileset(
            "fileset1",
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    when(dispatcher.loadFileset(any())).thenReturn(fileset);

    Response resp =
        target(filesetPath(metalake, catalog, schema) + "fileset1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    FilesetResponse filesetResp = resp.readEntity(FilesetResponse.class);
    Assertions.assertEquals(0, filesetResp.getCode());

    FilesetDTO filesetDTO = filesetResp.getFileset();
    Assertions.assertEquals(fileset.name(), filesetDTO.name());
    Assertions.assertEquals(fileset.type(), filesetDTO.type());
    Assertions.assertEquals(fileset.comment(), filesetDTO.comment());
    Assertions.assertEquals(fileset.properties(), filesetDTO.properties());
    Assertions.assertEquals(fileset.storageLocation(), filesetDTO.storageLocation());
    Assertions.assertEquals(fileset.storageLocations(), filesetDTO.storageLocations());

    // Test throw NoSuchFilesetException
    doThrow(new NoSuchFilesetException("no found")).when(dispatcher).loadFileset(any());
    Response resp1 =
        target(filesetPath(metalake, catalog, schema) + "fileset1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchFilesetException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("internal error")).when(dispatcher).loadFileset(any());
    Response resp2 =
        target(filesetPath(metalake, catalog, schema) + "fileset1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  @Test
  public void testCreateFileset() {
    Fileset fileset =
        mockFileset(
            "fileset1",
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    when(dispatcher.createMultipleLocationFileset(any(), any(), any(), any(), any()))
        .thenReturn(fileset);

    FilesetCreateRequest req =
        FilesetCreateRequest.builder()
            .name("fileset1")
            .comment("mock comment")
            .storageLocation("mock location")
            .properties(ImmutableMap.of("k1", "v1"))
            .build();

    Response resp =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    FilesetResponse filesetResp = resp.readEntity(FilesetResponse.class);
    Assertions.assertEquals(0, filesetResp.getCode());

    FilesetDTO filesetDTO = filesetResp.getFileset();
    Assertions.assertEquals("fileset1", filesetDTO.name());
    Assertions.assertEquals("mock comment", filesetDTO.comment());
    Assertions.assertEquals(Fileset.Type.MANAGED, filesetDTO.type());
    Assertions.assertEquals("mock location", filesetDTO.storageLocation());
    Assertions.assertEquals(
        ImmutableMap.of(LOCATION_NAME_UNKNOWN, "mock location"), filesetDTO.storageLocations());
    Assertions.assertEquals(ImmutableMap.of("k1", "v1"), filesetDTO.properties());

    // test multiple locations
    Map<String, String> locations =
        ImmutableMap.of(
            LOCATION_NAME_UNKNOWN,
            "mock default",
            "location1",
            "mock location1",
            "location2",
            "mock location2");
    fileset =
        mockMultipleLocationsFileset(
            "fileset1",
            Fileset.Type.MANAGED,
            "mock comment",
            locations,
            ImmutableMap.of("k1", "v1"));
    when(dispatcher.createMultipleLocationFileset(any(), any(), any(), any(), any()))
        .thenReturn(fileset);

    req =
        FilesetCreateRequest.builder()
            .name("fileset1")
            .comment("mock comment")
            .storageLocations(locations)
            .properties(ImmutableMap.of("k1", "v1"))
            .build();

    resp =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    filesetResp = resp.readEntity(FilesetResponse.class);
    Assertions.assertEquals(0, filesetResp.getCode());

    filesetDTO = filesetResp.getFileset();
    Assertions.assertEquals("fileset1", filesetDTO.name());
    Assertions.assertEquals("mock comment", filesetDTO.comment());
    Assertions.assertEquals(Fileset.Type.MANAGED, filesetDTO.type());
    Assertions.assertEquals("mock default", filesetDTO.storageLocation());
    Assertions.assertEquals(locations, filesetDTO.storageLocations());

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(dispatcher)
        .createMultipleLocationFileset(any(), any(), any(), any(), any());

    Response resp1 =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw FilesetAlreadyExistsException
    doThrow(new FilesetAlreadyExistsException("mock error"))
        .when(dispatcher)
        .createMultipleLocationFileset(any(), any(), any(), any(), any());

    Response resp2 =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp2.getCode());
    Assertions.assertEquals(
        FilesetAlreadyExistsException.class.getSimpleName(), errorResp2.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error"))
        .when(dispatcher)
        .createMultipleLocationFileset(any(), any(), any(), any(), any());

    Response resp3 =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp3.getStatus());

    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp3.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp3.getType());

    // Test throw Error
    doThrow(new Error("mock error"))
        .when(dispatcher)
        .createMultipleLocationFileset(any(), any(), any(), any(), any());
    Response resp4 =
        target(filesetPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp4.getStatus());
    ErrorResponse errorResp4 = resp4.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp4.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp4.getType());
  }

  @Test
  public void testRenameFileset() {
    FilesetUpdateRequest req = new FilesetUpdateRequest.RenameFilesetRequest("new name");
    Fileset fileset =
        mockFileset(
            "new name",
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    assertUpdateFileset(new FilesetUpdatesRequest(ImmutableList.of(req)), fileset);
  }

  @Test
  public void testSetFilesetProperties() {
    FilesetUpdateRequest req = new FilesetUpdateRequest.SetFilesetPropertiesRequest("k1", "k1");
    Fileset fileset =
        mockFileset(
            "fileset1",
            Fileset.Type.MANAGED,
            "mock comment",
            "mock location",
            ImmutableMap.of("k1", "v1"));
    assertUpdateFileset(new FilesetUpdatesRequest(ImmutableList.of(req)), fileset);
  }

  @Test
  public void testRemoveFilesetProperties() {
    FilesetUpdateRequest req = new FilesetUpdateRequest.RemoveFilesetPropertiesRequest("k1");
    Fileset fileset =
        mockFileset(
            "fileset1", Fileset.Type.MANAGED, "mock comment", "mock location", ImmutableMap.of());
    assertUpdateFileset(new FilesetUpdatesRequest(ImmutableList.of(req)), fileset);
  }

  @Test
  public void testUpdateFilesetComment() {
    FilesetUpdateRequest req = new FilesetUpdateRequest.UpdateFilesetCommentRequest("new comment");
    Fileset fileset =
        mockFileset(
            "fileset1", Fileset.Type.MANAGED, "new comment", "mock location", ImmutableMap.of());
    assertUpdateFileset(new FilesetUpdatesRequest(ImmutableList.of(req)), fileset);
  }

  @Test
  public void testRemoveFilesetComment() {
    FilesetUpdateRequest req = new FilesetUpdateRequest.UpdateFilesetCommentRequest(null);
    Fileset fileset =
        mockFileset("fileset1", Fileset.Type.MANAGED, null, "mock location", ImmutableMap.of());
    assertUpdateFileset(new FilesetUpdatesRequest(ImmutableList.of(req)), fileset);
  }

  @Test
  public void testMultiUpdateRequest() {
    FilesetUpdateRequest req = new FilesetUpdateRequest.RenameFilesetRequest("new name");
    FilesetUpdateRequest req1 = new FilesetUpdateRequest.UpdateFilesetCommentRequest("new comment");
    FilesetUpdateRequest req2 = new FilesetUpdateRequest.SetFilesetPropertiesRequest("k1", "v1");
    // update k1=v2
    FilesetUpdateRequest req3 = new FilesetUpdateRequest.SetFilesetPropertiesRequest("k1", "v2");
    FilesetUpdateRequest req4 = new FilesetUpdateRequest.SetFilesetPropertiesRequest("k2", "v2");
    // remove k2
    FilesetUpdateRequest req5 = new FilesetUpdateRequest.RemoveFilesetPropertiesRequest("k2");
    // remove comment
    FilesetUpdateRequest req6 = new FilesetUpdateRequest.UpdateFilesetCommentRequest(null);

    Fileset fileset =
        mockFileset(
            "new name", Fileset.Type.MANAGED, null, "mock location", ImmutableMap.of("k1", "v2"));
    assertUpdateFileset(
        new FilesetUpdatesRequest(ImmutableList.of(req, req1, req2, req3, req4, req5, req6)),
        fileset);
  }

  @Test
  public void testDropFileset() {
    when(dispatcher.dropFileset(any())).thenReturn(true);

    Response resp =
        target(filesetPath(metalake, catalog, schema) + "fileset1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    DropResponse dropResponse = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse.getCode());
    Assertions.assertTrue(dropResponse.dropped());

    when(dispatcher.dropFileset(any())).thenReturn(false);
    Response resp1 =
        target(filesetPath(metalake, catalog, schema) + "fileset1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    DropResponse dropResponse1 = resp1.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResponse1.getCode());
    Assertions.assertFalse(dropResponse1.dropped());

    when(dispatcher.dropFileset(any())).thenThrow(new RuntimeException("internal error"));
    Response resp2 =
        target(filesetPath(metalake, catalog, schema) + "fileset1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());
    ErrorResponse errorResp = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  public void testGetFileLocation() {
    // Test encoded subPath
    NameIdentifier fullIdentifier = NameIdentifier.of(metalake, catalog, schema, "fileset1");
    String subPath = "/test/1";
    when(dispatcher.getFileLocation(fullIdentifier, subPath, null)).thenReturn(subPath);
    Response resp =
        target(filesetPath(metalake, catalog, schema) + "fileset1/location")
            .queryParam("sub_path", RESTUtils.encodeString(subPath))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    FileLocationResponse contextResponse = resp.readEntity(FileLocationResponse.class);
    Assertions.assertEquals(0, contextResponse.getCode());

    Assertions.assertEquals(subPath, contextResponse.getFileLocation());

    // test specify location name
    String locationName = "location1";
    String location1 = "/test/location1";
    when(dispatcher.getFileLocation(fullIdentifier, subPath, locationName)).thenReturn(location1);
    resp =
        target(filesetPath(metalake, catalog, schema) + "fileset1/location")
            .queryParam("sub_path", RESTUtils.encodeString(subPath))
            .queryParam("location_name", locationName)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    contextResponse = resp.readEntity(FileLocationResponse.class);
    Assertions.assertEquals(0, contextResponse.getCode());
    Assertions.assertEquals(location1, contextResponse.getFileLocation());

    // Test throw NoSuchFilesetException
    doThrow(new NoSuchFilesetException("no found"))
        .when(dispatcher)
        .getFileLocation(fullIdentifier, subPath, null);
    Response resp1 =
        target(filesetPath(metalake, catalog, schema) + "fileset1/location")
            .queryParam("sub_path", RESTUtils.encodeString(subPath))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchFilesetException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("internal error"))
        .when(dispatcher)
        .getFileLocation(fullIdentifier, subPath, null);
    Response resp2 =
        target(filesetPath(metalake, catalog, schema) + "fileset1/location")
            .queryParam("sub_path", RESTUtils.encodeString(subPath))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());

    // Test not encoded subPath
    NameIdentifier fullIdentifier1 = NameIdentifier.of(metalake, catalog, schema, "fileset2");
    String subPath1 = "/test/2";
    when(dispatcher.getFileLocation(fullIdentifier1, subPath1, null)).thenReturn(subPath1);
    Response resp3 =
        target(filesetPath(metalake, catalog, schema) + "fileset2/location")
            .queryParam("sub_path", subPath1)
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());

    FileLocationResponse contextResponse1 = resp3.readEntity(FileLocationResponse.class);
    Assertions.assertEquals(0, contextResponse1.getCode());

    Assertions.assertEquals(subPath1, contextResponse1.getFileLocation());

    // test throw NoSuchLocationNameException
    doThrow(new NoSuchLocationNameException("no found"))
        .when(dispatcher)
        .getFileLocation(fullIdentifier, subPath, "not_exist");
    Response noNameResp =
        target(filesetPath(metalake, catalog, schema) + "fileset1/location")
            .queryParam("sub_path", RESTUtils.encodeString(subPath))
            .queryParam("location_name", "not_exist")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), noNameResp.getStatus());

    ErrorResponse errorResp4 = noNameResp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp4.getCode());
    Assertions.assertEquals(
        NoSuchLocationNameException.class.getSimpleName(), errorResp4.getType());

    // Test header to caller context
    try {
      Map<String, String> callerContextMap = Maps.newHashMap();
      NameIdentifier fullIdentifier2 = NameIdentifier.of(metalake, catalog, schema, "fileset3");
      String subPath2 = "/test/3";
      when(dispatcher.getFileLocation(fullIdentifier2, subPath2, null))
          .thenAnswer(
              (Answer<String>)
                  invocation -> {
                    try {
                      CallerContext context = CallerContext.CallerContextHolder.get();
                      callerContextMap.putAll(context.context());
                      return subPath2;
                    } finally {
                      CallerContext.CallerContextHolder.remove();
                    }
                  });
      Response resp4 =
          target(filesetPath(metalake, catalog, schema) + "fileset3/location")
              .queryParam("sub_path", RESTUtils.encodeString(subPath2))
              .request(MediaType.APPLICATION_JSON_TYPE)
              .header(
                  FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE,
                  InternalClientType.HADOOP_GVFS.name())
              .header(
                  FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION,
                  FilesetDataOperation.CREATE.name())
              .accept("application/vnd.gravitino.v1+json")
              .get();
      Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp3.getStatus());

      FileLocationResponse contextResponse2 = resp4.readEntity(FileLocationResponse.class);
      Assertions.assertEquals(0, contextResponse2.getCode());

      Assertions.assertEquals(subPath2, contextResponse2.getFileLocation());

      Assertions.assertFalse(callerContextMap.isEmpty());
      Assertions.assertEquals(
          InternalClientType.HADOOP_GVFS.name(),
          callerContextMap.get(FilesetAuditConstants.HTTP_HEADER_INTERNAL_CLIENT_TYPE));
      Assertions.assertEquals(
          FilesetDataOperation.CREATE.name(),
          callerContextMap.get(FilesetAuditConstants.HTTP_HEADER_FILESET_DATA_OPERATION));
    } finally {
      CallerContext.CallerContextHolder.remove();
    }

    // test `getFileLocation` clear caller context finally
    Map<String, String> testContextMap = Maps.newHashMap();
    testContextMap.put("k", "v");
    CallerContext context = CallerContext.builder().withContext(testContextMap).build();
    CallerContext.CallerContextHolder.set(context);
    FilesetOperations mockOperations = Mockito.mock(FilesetOperations.class);
    Mockito.when(mockOperations.getFileLocation(any(), any(), any(), any(), any(), any()))
        .thenCallRealMethod();
    mockOperations.getFileLocation(
        "test_metalake", "test_catalog", "test_schema", "fileset4", "/test", "default");
    Assertions.assertNull(CallerContext.CallerContextHolder.get());
  }

  private void assertUpdateFileset(FilesetUpdatesRequest req, Fileset updatedFileset) {
    when(dispatcher.alterFileset(any(), any(FilesetChange.class))).thenReturn(updatedFileset);

    Response resp1 =
        target(filesetPath(metalake, catalog, schema) + "fileset1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    FilesetResponse filesetResp = resp1.readEntity(FilesetResponse.class);
    Assertions.assertEquals(0, filesetResp.getCode());

    FilesetDTO filesetDTO = filesetResp.getFileset();
    Assertions.assertEquals(updatedFileset.name(), filesetDTO.name());
    Assertions.assertEquals(updatedFileset.comment(), filesetDTO.comment());
    Assertions.assertEquals(updatedFileset.type(), filesetDTO.type());
    Assertions.assertEquals(updatedFileset.properties(), filesetDTO.properties());
  }

  private static String filesetPath(String metalake, String catalog, String schema) {
    return new StringBuilder()
        .append("/metalakes/")
        .append(metalake)
        .append("/catalogs/")
        .append(catalog)
        .append("/schemas/")
        .append(schema)
        .append("/filesets/")
        .toString();
  }

  private static Fileset mockFileset(
      String filesetName,
      Fileset.Type type,
      String comment,
      String storageLocation,
      Map<String, String> properties) {
    Fileset mockFileset = mock(Fileset.class);
    when(mockFileset.name()).thenReturn(filesetName);
    when(mockFileset.type()).thenReturn(type);
    when(mockFileset.comment()).thenReturn(comment);
    when(mockFileset.storageLocation()).thenCallRealMethod();
    when(mockFileset.storageLocations())
        .thenReturn(ImmutableMap.of(LOCATION_NAME_UNKNOWN, storageLocation));
    when(mockFileset.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockFileset.auditInfo()).thenReturn(mockAudit);

    return mockFileset;
  }

  private static Fileset mockMultipleLocationsFileset(
      String filesetName,
      Fileset.Type type,
      String comment,
      Map<String, String> storageLocations,
      Map<String, String> properties) {
    Fileset mockFileset = mock(Fileset.class);
    when(mockFileset.name()).thenReturn(filesetName);
    when(mockFileset.type()).thenReturn(type);
    when(mockFileset.comment()).thenReturn(comment);
    when(mockFileset.storageLocation()).thenCallRealMethod();
    when(mockFileset.storageLocations()).thenReturn(storageLocations);
    when(mockFileset.properties()).thenReturn(properties);

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockFileset.auditInfo()).thenReturn(mockAudit);

    return mockFileset;
  }
}
