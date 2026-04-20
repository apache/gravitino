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

import static org.apache.gravitino.Configs.CACHE_ENABLED;
import static org.apache.gravitino.Configs.ENABLE_AUTHORIZATION;
import static org.apache.gravitino.Configs.TREE_LOCK_CLEAN_INTERVAL;
import static org.apache.gravitino.Configs.TREE_LOCK_MAX_NODE_IN_MEMORY;
import static org.apache.gravitino.Configs.TREE_LOCK_MIN_NODE_IN_MEMORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.dto.rel.ViewDTO;
import org.apache.gravitino.dto.requests.ViewCreateRequest;
import org.apache.gravitino.dto.requests.ViewUpdateRequest;
import org.apache.gravitino.dto.requests.ViewUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorConstants;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.ViewResponse;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.lock.LockManager;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rest.RESTUtils;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.test.TestProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestViewOperations extends BaseOperationsTest {

  private static class MockServletRequestFactory extends ServletRequestFactoryBase {
    @Override
    public HttpServletRequest get() {
      HttpServletRequest request = mock(HttpServletRequest.class);
      when(request.getRemoteUser()).thenReturn(null);
      return request;
    }
  }

  private static SchemaDispatcher schemaDispatcher = mock(SchemaDispatcher.class);
  private final ViewDispatcher dispatcher = mock(ViewDispatcher.class);
  private final String metalake = "metalake";
  private final String catalog = "catalog1";
  private final String schema = "default";

  @BeforeAll
  public static void setup() throws IllegalAccessException {
    Config config = mock(Config.class);
    Mockito.doReturn(100000L).when(config).get(TREE_LOCK_MAX_NODE_IN_MEMORY);
    Mockito.doReturn(1000L).when(config).get(TREE_LOCK_MIN_NODE_IN_MEMORY);
    Mockito.doReturn(36000L).when(config).get(TREE_LOCK_CLEAN_INTERVAL);
    Mockito.doReturn(false).when(config).get(CACHE_ENABLED);
    Mockito.doReturn(false).when(config).get(ENABLE_AUTHORIZATION);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "config", config, true);
    FieldUtils.writeField(GravitinoEnv.getInstance(), "lockManager", new LockManager(config), true);
    Mockito.doReturn(true).when(schemaDispatcher).schemaExists(any());
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
    resourceConfig.register(ViewOperations.class);
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            bind(dispatcher).to(ViewDispatcher.class).ranked(2);
            bindFactory(TestViewOperations.MockServletRequestFactory.class)
                .to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @Test
  public void testListViews() {
    NameIdentifier view1 = NameIdentifier.of(metalake, catalog, schema, "view1");
    NameIdentifier view2 = NameIdentifier.of(metalake, catalog, schema, "view2");

    when(dispatcher.listViews(any())).thenReturn(new NameIdentifier[] {view1, view2});

    Response resp =
        target(viewPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    Assertions.assertEquals(MediaType.APPLICATION_JSON_TYPE, resp.getMediaType());

    EntityListResponse listResp = resp.readEntity(EntityListResponse.class);
    Assertions.assertEquals(0, listResp.getCode());

    NameIdentifier[] views = listResp.identifiers();
    Assertions.assertEquals(2, views.length);
    Assertions.assertEquals(view1, views[0]);
    Assertions.assertEquals(view2, views[1]);

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error")).when(dispatcher).listViews(any());
    Response resp1 =
        target(viewPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();

    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchSchemaException.class.getSimpleName(), errorResp.getType());

    // Test throw RuntimeException
    doThrow(new RuntimeException("mock error")).when(dispatcher).listViews(any());
    Response resp2 =
        target(viewPath(metalake, catalog, schema))
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
  public void testLoadView() {
    View view = mockView("view1", "comment", ImmutableMap.of("k", "v"), "trino", "SELECT 1");
    when(dispatcher.loadView(any())).thenReturn(view);

    Response resp =
        target(viewPath(metalake, catalog, schema) + "/view1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    ViewResponse viewResp = resp.readEntity(ViewResponse.class);
    Assertions.assertEquals(0, viewResp.getCode());

    ViewDTO viewDTO = viewResp.getView();
    Assertions.assertEquals("view1", viewDTO.name());
    Assertions.assertEquals("comment", viewDTO.comment());
    Assertions.assertEquals(ImmutableMap.of("k", "v"), viewDTO.properties());
    Assertions.assertNull(viewDTO.defaultCatalog());
    Assertions.assertNull(viewDTO.defaultSchema());
    Assertions.assertEquals(1, viewDTO.representations().length);
    Assertions.assertTrue(viewDTO.representations()[0] instanceof SQLRepresentationDTO);
    SQLRepresentationDTO sqlRep = (SQLRepresentationDTO) viewDTO.representations()[0];
    Assertions.assertEquals("trino", sqlRep.dialect());
    Assertions.assertEquals("SELECT 1", sqlRep.sql());

    // Test throw NoSuchViewException
    doThrow(new NoSuchViewException("mock error")).when(dispatcher).loadView(any());
    Response resp1 =
        target(viewPath(metalake, catalog, schema) + "/view1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .get();
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.NOT_FOUND_CODE, errorResp.getCode());
    Assertions.assertEquals(NoSuchViewException.class.getSimpleName(), errorResp.getType());
  }

  @Test
  public void testCreateView() {
    View view = mockView("view1", "comment", ImmutableMap.of("k", "v"), "trino", "SELECT 1");
    when(dispatcher.createView(any(), any(), any(), any(), any(), any(), any())).thenReturn(view);

    SQLRepresentationDTO repDTO =
        SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build();
    ViewCreateRequest req =
        ViewCreateRequest.builder()
            .name("view1")
            .comment("comment")
            .columns(new ColumnDTO[0])
            .representations(new SQLRepresentationDTO[] {repDTO})
            .properties(ImmutableMap.of("k", "v"))
            .build();

    Response resp =
        target(viewPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    ViewResponse viewResp = resp.readEntity(ViewResponse.class);
    Assertions.assertEquals(0, viewResp.getCode());
    Assertions.assertEquals("view1", viewResp.getView().name());

    // Test throw ViewAlreadyExistsException
    doThrow(new ViewAlreadyExistsException("mock error"))
        .when(dispatcher)
        .createView(any(), any(), any(), any(), any(), any(), any());
    Response resp1 =
        target(viewPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.CONFLICT.getStatusCode(), resp1.getStatus());

    ErrorResponse errorResp1 = resp1.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ALREADY_EXISTS_CODE, errorResp1.getCode());
    Assertions.assertEquals(ViewAlreadyExistsException.class.getSimpleName(), errorResp1.getType());

    // Test throw NoSuchSchemaException
    doThrow(new NoSuchSchemaException("mock error"))
        .when(dispatcher)
        .createView(any(), any(), any(), any(), any(), any(), any());
    Response resp2 =
        target(viewPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp2.getStatus());

    // Test duplicate SQL representation dialects → 400 BAD_REQUEST
    SQLRepresentationDTO dupRep1 =
        SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build();
    SQLRepresentationDTO dupRep2 =
        SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 2").build();
    ViewCreateRequest dupReq =
        ViewCreateRequest.builder()
            .name("view1")
            .columns(new ColumnDTO[0])
            .representations(new SQLRepresentationDTO[] {dupRep1, dupRep2})
            .build();
    Response resp3 =
        target(viewPath(metalake, catalog, schema))
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .post(Entity.entity(dupReq, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp3.getStatus());
    ErrorResponse errorResp3 = resp3.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp3.getCode());
  }

  @Test
  public void testRenameView() {
    ViewUpdateRequest req = new ViewUpdateRequest.RenameViewRequest("view2");
    View view = mockView("view2", "comment", ImmutableMap.of(), "trino", "SELECT 1");
    assertUpdateView(new ViewUpdatesRequest(ImmutableList.of(req)), view);
  }

  @Test
  public void testSetAndRemoveViewProperty() {
    ViewUpdateRequest req1 = new ViewUpdateRequest.SetViewPropertyRequest("k", "v");
    ViewUpdateRequest req2 = new ViewUpdateRequest.RemoveViewPropertyRequest("k");
    View view = mockView("view1", "comment", ImmutableMap.of(), "trino", "SELECT 1");
    assertUpdateView(new ViewUpdatesRequest(ImmutableList.of(req1, req2)), view);
  }

  @Test
  public void testReplaceView() {
    SQLRepresentationDTO repDTO =
        SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 2").build();
    ViewUpdateRequest replace =
        new ViewUpdateRequest.ReplaceViewRequest(
            new ColumnDTO[0], new SQLRepresentationDTO[] {repDTO}, "cat1", "sch1", "new comment");
    View view = mockView("view1", "new comment", ImmutableMap.of(), "trino", "SELECT 2");
    assertUpdateView(new ViewUpdatesRequest(ImmutableList.of(replace)), view);

    // Test duplicate SQL representation dialects → 400 BAD_REQUEST
    SQLRepresentationDTO dupRep1 =
        SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 1").build();
    SQLRepresentationDTO dupRep2 =
        SQLRepresentationDTO.builder().withDialect("trino").withSql("SELECT 2").build();
    ViewUpdateRequest dupReplace =
        new ViewUpdateRequest.ReplaceViewRequest(
            new ColumnDTO[0], new SQLRepresentationDTO[] {dupRep1, dupRep2}, null, null, null);
    Response resp =
        target(viewPath(metalake, catalog, schema) + "/view1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(
                Entity.entity(
                    new ViewUpdatesRequest(ImmutableList.of(dupReplace)),
                    MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    ErrorResponse errorResp = resp.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.ILLEGAL_ARGUMENTS_CODE, errorResp.getCode());
  }

  @Test
  public void testDropView() {
    when(dispatcher.dropView(any())).thenReturn(true);
    Response resp =
        target(viewPath(metalake, catalog, schema) + "/view1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    DropResponse dropResp = resp.readEntity(DropResponse.class);
    Assertions.assertEquals(0, dropResp.getCode());
    Assertions.assertTrue(dropResp.dropped());

    when(dispatcher.dropView(any())).thenReturn(false);
    Response resp1 =
        target(viewPath(metalake, catalog, schema) + "/view1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp1.getStatus());

    DropResponse dropResp1 = resp1.readEntity(DropResponse.class);
    Assertions.assertFalse(dropResp1.dropped());

    doThrow(new RuntimeException("mock error")).when(dispatcher).dropView(any());
    Response resp2 =
        target(viewPath(metalake, catalog, schema) + "/view1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .delete();

    Assertions.assertEquals(
        Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), resp2.getStatus());

    ErrorResponse errorResp2 = resp2.readEntity(ErrorResponse.class);
    Assertions.assertEquals(ErrorConstants.INTERNAL_ERROR_CODE, errorResp2.getCode());
    Assertions.assertEquals(RuntimeException.class.getSimpleName(), errorResp2.getType());
  }

  private void assertUpdateView(ViewUpdatesRequest req, View updatedView) {
    when(dispatcher.alterView(any(), any(ViewChange[].class))).thenReturn(updatedView);

    Response resp =
        target(viewPath(metalake, catalog, schema) + "/view1")
            .request(MediaType.APPLICATION_JSON_TYPE)
            .accept("application/vnd.gravitino.v1+json")
            .put(Entity.entity(req, MediaType.APPLICATION_JSON_TYPE));
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    ViewResponse viewResp = resp.readEntity(ViewResponse.class);
    Assertions.assertEquals(0, viewResp.getCode());

    ViewDTO dto = viewResp.getView();
    Assertions.assertEquals(updatedView.name(), dto.name());
    Assertions.assertEquals(updatedView.comment(), dto.comment());
    Assertions.assertEquals(updatedView.properties(), dto.properties());
  }

  private View mockView(
      String name, String comment, Map<String, String> properties, String dialect, String sql) {
    View mockedView = mock(View.class);
    when(mockedView.name()).thenReturn(name);
    when(mockedView.comment()).thenReturn(comment);
    when(mockedView.properties()).thenReturn(properties);
    when(mockedView.columns()).thenReturn(new Column[0]);
    SQLRepresentation rep = SQLRepresentation.builder().withDialect(dialect).withSql(sql).build();
    when(mockedView.representations()).thenReturn(new Representation[] {rep});

    Audit mockAudit = mock(Audit.class);
    when(mockAudit.creator()).thenReturn("gravitino");
    when(mockAudit.createTime()).thenReturn(Instant.now());
    when(mockedView.auditInfo()).thenReturn(mockAudit);

    return mockedView;
  }

  private String viewPath(String metalake, String catalog, String schema) {
    return "/metalakes/" + metalake + "/catalogs/" + catalog + "/schemas/" + schema + "/views";
  }
}
