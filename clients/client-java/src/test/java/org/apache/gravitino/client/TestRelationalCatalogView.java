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

import static org.apache.hc.core5.http.HttpStatus.SC_CONFLICT;
import static org.apache.hc.core5.http.HttpStatus.SC_INTERNAL_SERVER_ERROR;
import static org.apache.hc.core5.http.HttpStatus.SC_NOT_FOUND;
import static org.apache.hc.core5.http.HttpStatus.SC_OK;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.rel.ColumnDTO;
import org.apache.gravitino.dto.rel.RepresentationDTO;
import org.apache.gravitino.dto.rel.SQLRepresentationDTO;
import org.apache.gravitino.dto.rel.ViewDTO;
import org.apache.gravitino.dto.requests.ViewCreateRequest;
import org.apache.gravitino.dto.requests.ViewUpdateRequest;
import org.apache.gravitino.dto.requests.ViewUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ErrorResponse;
import org.apache.gravitino.dto.responses.ViewResponse;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchViewException;
import org.apache.gravitino.exceptions.ViewAlreadyExistsException;
import org.apache.gravitino.rel.Dialects;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewCatalog;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.rel.types.Types;
import org.apache.hc.core5.http.Method;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for view operations exposed by {@link RelationalCatalog}. */
public class TestRelationalCatalogView extends TestRelationalCatalog {

  private static final String SCHEMA_NAME = "schema1";

  @Test
  public void testListViews() throws JsonProcessingException {
    NameIdentifier view1 = NameIdentifier.of(metalakeName, catalogName, SCHEMA_NAME, "view1");
    NameIdentifier view2 = NameIdentifier.of(metalakeName, catalogName, SCHEMA_NAME, "view2");
    String viewPath = withSlash(RelationalCatalog.formatViewRequestPath(view1.namespace()));

    // empty result
    EntityListResponse emptyResp = new EntityListResponse(new NameIdentifier[] {});
    buildMockResource(Method.GET, viewPath, null, emptyResp, SC_OK);
    NameIdentifier[] empty = catalog.asViewCatalog().listViews(Namespace.of(SCHEMA_NAME));
    Assertions.assertEquals(0, empty.length);

    // some results
    EntityListResponse resp = new EntityListResponse(new NameIdentifier[] {view1, view2});
    buildMockResource(Method.GET, viewPath, null, resp, SC_OK);
    NameIdentifier[] views = catalog.asViewCatalog().listViews(Namespace.of(SCHEMA_NAME));
    Assertions.assertEquals(2, views.length);
    Assertions.assertEquals(NameIdentifier.of(SCHEMA_NAME, "view1"), views[0]);
    Assertions.assertEquals(NameIdentifier.of(SCHEMA_NAME, "view2"), views[1]);

    // NoSuchSchemaException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.GET, viewPath, null, errorResp, SC_NOT_FOUND);
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    Namespace ns = Namespace.of(SCHEMA_NAME);
    Throwable ex =
        Assertions.assertThrows(NoSuchSchemaException.class, () -> viewCatalog.listViews(ns));
    Assertions.assertTrue(ex.getMessage().contains("schema not found"));

    // Internal error
    ErrorResponse errorResp1 = ErrorResponse.internalError("internal error");
    buildMockResource(Method.GET, viewPath, null, errorResp1, SC_INTERNAL_SERVER_ERROR);
    Throwable ex1 =
        Assertions.assertThrows(RuntimeException.class, () -> viewCatalog.listViews(ns));
    Assertions.assertTrue(ex1.getMessage().contains("internal error"));
  }

  @Test
  public void testLoadView() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, SCHEMA_NAME);
    String viewPath =
        withSlash(RelationalCatalog.formatViewRequestPath(fullNamespace) + "/" + ident.name());

    ViewDTO expected = newViewDTO("view1", "comment", defaultColumns(), defaultRepresentations());
    ViewResponse resp = new ViewResponse(expected);
    buildMockResource(Method.GET, viewPath, null, resp, SC_OK);

    View loaded = catalog.asViewCatalog().loadView(ident);
    Assertions.assertEquals(expected.name(), loaded.name());
    Assertions.assertEquals(expected.comment(), loaded.comment());
    Assertions.assertEquals(expected.defaultCatalog(), loaded.defaultCatalog());
    Assertions.assertEquals(expected.defaultSchema(), loaded.defaultSchema());
    Assertions.assertEquals(expected.representations().length, loaded.representations().length);

    // NoSuchViewException
    ErrorResponse errorResp =
        ErrorResponse.notFound(NoSuchViewException.class.getSimpleName(), "view not found");
    buildMockResource(Method.GET, viewPath, null, errorResp, SC_NOT_FOUND);
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    Throwable ex =
        Assertions.assertThrows(NoSuchViewException.class, () -> viewCatalog.loadView(ident));
    Assertions.assertTrue(ex.getMessage().contains("view not found"));
  }

  @Test
  public void testCreateView() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, SCHEMA_NAME);
    String viewPath = withSlash(RelationalCatalog.formatViewRequestPath(fullNamespace));

    ColumnDTO[] columns = defaultColumns();
    RepresentationDTO[] representations = defaultRepresentations();
    Map<String, String> properties = ImmutableMap.of("k1", "v1");

    ViewCreateRequest req =
        new ViewCreateRequest(
            ident.name(), "comment", columns, representations, null, null, properties);
    ViewDTO expected = newViewDTO("view1", "comment", columns, representations);
    ViewResponse resp = new ViewResponse(expected);
    buildMockResource(Method.POST, viewPath, req, resp, SC_OK);

    Representation[] apiRepresentations =
        new Representation[] {
          SQLRepresentation.builder().withDialect(Dialects.TRINO).withSql("SELECT 1").build()
        };
    View created =
        catalog
            .asViewCatalog()
            .createView(ident, "comment", columns, apiRepresentations, null, null, properties);
    Assertions.assertEquals(expected.name(), created.name());
    Assertions.assertEquals(expected.comment(), created.comment());

    // NoSuchSchemaException
    ErrorResponse schemaErr =
        ErrorResponse.notFound(NoSuchSchemaException.class.getSimpleName(), "schema not found");
    buildMockResource(Method.POST, viewPath, req, schemaErr, SC_NOT_FOUND);
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    Throwable ex =
        Assertions.assertThrows(
            NoSuchSchemaException.class,
            () ->
                viewCatalog.createView(
                    ident, "comment", columns, apiRepresentations, null, null, properties));
    Assertions.assertTrue(ex.getMessage().contains("schema not found"));

    // ViewAlreadyExistsException
    ErrorResponse existsErr =
        ErrorResponse.alreadyExists(
            ViewAlreadyExistsException.class.getSimpleName(), "view already exists");
    buildMockResource(Method.POST, viewPath, req, existsErr, SC_CONFLICT);
    Throwable ex1 =
        Assertions.assertThrows(
            ViewAlreadyExistsException.class,
            () ->
                viewCatalog.createView(
                    ident, "comment", columns, apiRepresentations, null, null, properties));
    Assertions.assertTrue(ex1.getMessage().contains("view already exists"));
  }

  @Test
  public void testAlterViewRename() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");
    ViewDTO updated = newViewDTO("view2", "comment", defaultColumns(), defaultRepresentations());
    ViewUpdateRequest req = new ViewUpdateRequest.RenameViewRequest("view2");
    runAlterView(ident, req, updated, ViewChange.rename("view2"));
  }

  @Test
  public void testAlterViewSetProperty() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");
    ViewDTO updated =
        ViewDTO.builder()
            .withName("view1")
            .withComment("comment")
            .withColumns(defaultColumns())
            .withRepresentations(defaultRepresentations())
            .withProperties(ImmutableMap.of("k1", "v1"))
            .withAudit(mockAudit())
            .build();
    ViewUpdateRequest req = new ViewUpdateRequest.SetViewPropertyRequest("k1", "v1");
    runAlterView(ident, req, updated, ViewChange.setProperty("k1", "v1"));
  }

  @Test
  public void testAlterViewRemoveProperty() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");
    ViewDTO updated = newViewDTO("view1", "comment", defaultColumns(), defaultRepresentations());
    ViewUpdateRequest req = new ViewUpdateRequest.RemoveViewPropertyRequest("k1");
    runAlterView(ident, req, updated, ViewChange.removeProperty("k1"));
  }

  @Test
  public void testAlterViewReplace() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");
    SQLRepresentationDTO newRep =
        SQLRepresentationDTO.builder().withDialect(Dialects.TRINO).withSql("SELECT 3").build();
    ColumnDTO[] newColumns = defaultColumns();
    RepresentationDTO[] newReps = new RepresentationDTO[] {newRep};
    ViewDTO updated = newViewDTO("view1", "new comment", newColumns, newReps);

    ViewUpdateRequest req =
        new ViewUpdateRequest.ReplaceViewRequest(newColumns, newReps, null, null, "new comment");
    SQLRepresentation newRepApi =
        SQLRepresentation.builder().withDialect(Dialects.TRINO).withSql("SELECT 3").build();
    runAlterView(
        ident,
        req,
        updated,
        ViewChange.replaceView(
            newColumns, new Representation[] {newRepApi}, null, null, "new comment"));
  }

  @Test
  public void testDropView() throws JsonProcessingException {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, SCHEMA_NAME);
    String viewPath =
        withSlash(RelationalCatalog.formatViewRequestPath(fullNamespace) + "/" + ident.name());

    DropResponse resp = new DropResponse(true);
    buildMockResource(Method.DELETE, viewPath, null, resp, SC_OK);
    Assertions.assertTrue(catalog.asViewCatalog().dropView(ident));

    // return false
    DropResponse resp2 = new DropResponse(false);
    buildMockResource(Method.DELETE, viewPath, null, resp2, SC_OK);
    Assertions.assertFalse(catalog.asViewCatalog().dropView(ident));

    // internal error
    ErrorResponse errorResp = ErrorResponse.internalError("internal error");
    buildMockResource(Method.DELETE, viewPath, null, errorResp, SC_INTERNAL_SERVER_ERROR);
    Throwable ex =
        Assertions.assertThrows(
            RuntimeException.class, () -> catalog.asViewCatalog().dropView(ident));
    Assertions.assertTrue(ex.getMessage().contains("internal error"));
  }

  @Test
  public void testCreateViewRejectsNullRepresentations() {
    NameIdentifier ident = NameIdentifier.of(SCHEMA_NAME, "view1");

    ViewCatalog viewCatalog = catalog.asViewCatalog();
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                viewCatalog.createView(
                    ident, "comment", null, null, null, null, Collections.emptyMap()));
    Assertions.assertTrue(
        exception
            .getMessage()
            .contains("\"representations\" field is required and cannot be empty"));
  }

  @Test
  public void testListViewsRejectsInvalidNamespace() {
    ViewCatalog viewCatalog = catalog.asViewCatalog();
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> viewCatalog.listViews(Namespace.of("s1", "s2")));
    Assertions.assertTrue(exception.getMessage().contains("have 1 level"));
  }

  @Test
  public void testFormatViewRequestPathRejectsInvalidNamespaceLength() {
    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> RelationalCatalog.formatViewRequestPath(Namespace.of("schema1")));
    Assertions.assertTrue(exception.getMessage().contains("have 3 levels"));
  }

  private void runAlterView(
      NameIdentifier ident, ViewUpdateRequest req, ViewDTO updated, ViewChange change)
      throws JsonProcessingException {
    Namespace fullNamespace = Namespace.of(metalakeName, catalogName, ident.namespace().level(0));
    String viewPath =
        withSlash(RelationalCatalog.formatViewRequestPath(fullNamespace) + "/" + ident.name());
    ViewUpdatesRequest updatesRequest = new ViewUpdatesRequest(ImmutableList.of(req));
    ViewResponse resp = new ViewResponse(updated);
    buildMockResource(Method.PUT, viewPath, updatesRequest, resp, SC_OK);

    View altered = catalog.asViewCatalog().alterView(ident, change);
    Assertions.assertEquals(updated.name(), altered.name());
    Assertions.assertEquals(updated.comment(), altered.comment());
    Assertions.assertEquals(updated.properties(), altered.properties());
    Assertions.assertEquals(updated.representations().length, altered.representations().length);
  }

  private static ColumnDTO[] defaultColumns() {
    return new ColumnDTO[] {createMockColumn("col1", Types.IntegerType.get(), "c1")};
  }

  private static RepresentationDTO[] defaultRepresentations() {
    return new RepresentationDTO[] {
      SQLRepresentationDTO.builder().withDialect(Dialects.TRINO).withSql("SELECT 1").build()
    };
  }

  private static ViewDTO newViewDTO(
      String name, String comment, ColumnDTO[] columns, RepresentationDTO[] representations) {
    return ViewDTO.builder()
        .withName(name)
        .withComment(comment)
        .withColumns(columns)
        .withRepresentations(representations)
        .withProperties(Collections.emptyMap())
        .withAudit(mockAudit())
        .build();
  }

  private static AuditDTO mockAudit() {
    return AuditDTO.builder().withCreator("creator").withCreateTime(Instant.now()).build();
  }
}
