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

package org.apache.gravitino.iceberg.service.rest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.listener.api.event.Event;
import org.apache.gravitino.listener.api.event.IcebergCreateViewEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergCreateViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergDropViewEvent;
import org.apache.gravitino.listener.api.event.IcebergDropViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergDropViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergListViewEvent;
import org.apache.gravitino.listener.api.event.IcebergListViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergListViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadViewEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergLoadViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameViewEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergRenameViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergReplaceViewEvent;
import org.apache.gravitino.listener.api.event.IcebergReplaceViewFailureEvent;
import org.apache.gravitino.listener.api.event.IcebergReplaceViewPreEvent;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.listener.api.event.IcebergViewExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergViewExistsPreEvent;
import org.apache.gravitino.listener.api.event.OperationType;
import org.apache.gravitino.listener.api.event.PreEvent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.ViewMetadata;
import org.glassfish.jersey.internal.inject.AbstractBinder;
import org.glassfish.jersey.server.ResourceConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

@SuppressWarnings("deprecation")
public class TestIcebergViewOperations extends IcebergNamespaceTestBase {
  private static final Schema viewSchema =
      new Schema(Types.NestedField.of(1, false, "foo_string", Types.StringType.get()));

  private static final Schema newViewSchema =
      new Schema(Types.NestedField.of(2, false, "foo_string1", Types.StringType.get()));

  private static final String VIEW_QUERY = "select 1";

  private DummyEventListener dummyEventListener;

  @Override
  protected Application configure() {
    this.dummyEventListener = new DummyEventListener();
    ResourceConfig resourceConfig =
        IcebergRestTestUtil.getIcebergResourceConfig(
            MockIcebergViewOperations.class, true, Arrays.asList(dummyEventListener));
    // create namespace before each view test
    resourceConfig.register(MockIcebergNamespaceOperations.class);
    resourceConfig.register(MockIcebergViewRenameOperations.class);

    // register a mock HttpServletRequest with user info
    resourceConfig.register(
        new AbstractBinder() {
          @Override
          protected void configure() {
            HttpServletRequest mockRequest = Mockito.mock(HttpServletRequest.class);
            Mockito.when(mockRequest.getUserPrincipal()).thenReturn(() -> "test-user");
            bind(mockRequest).to(HttpServletRequest.class);
          }
        });

    return resourceConfig;
  }

  @ParameterizedTest
  @MethodSource(
      "org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testPrefixesAndNamespaces")
  void testListViews(String prefix, Namespace namespace) {
    setUrlPathWithPrefix(prefix);
    verifyListViewFail(namespace, 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergListViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergListViewFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "list_foo1");
    verifyCreateViewSucc(namespace, "list_foo2");

    dummyEventListener.clearEvent();
    verifyLisViewSucc(namespace, ImmutableSet.of("list_foo1", "list_foo2"));
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergListViewPreEvent);
    Event listViewPostEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(listViewPostEvent instanceof IcebergListViewEvent);
    Assertions.assertEquals(2, ((IcebergListViewEvent) listViewPostEvent).resultCount());
  }

  @ParameterizedTest
  @MethodSource(
      "org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testPrefixesAndNamespaces")
  void testListViewsWithPagination(String prefix, Namespace namespace) {
    setUrlPathWithPrefix(prefix);
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "page_v1");
    verifyCreateViewSucc(namespace, "page_v2");
    verifyCreateViewSucc(namespace, "page_v3");

    dummyEventListener.clearEvent();

    // First page: pageSize=2
    String viewPath =
        IcebergRestTestUtil.NAMESPACE_PATH + "/" + RESTUtil.encodeNamespace(namespace) + "/views";
    Response firstPageResponse =
        getIcebergClientBuilder(viewPath, Optional.of(ImmutableMap.of("pageSize", "2"))).get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), firstPageResponse.getStatus());
    ListTablesResponse firstPage = firstPageResponse.readEntity(ListTablesResponse.class);
    Assertions.assertEquals(2, firstPage.identifiers().size());
    Assertions.assertNotNull(firstPage.nextPageToken());

    // Second page using nextPageToken
    Response secondPageResponse =
        getIcebergClientBuilder(
                viewPath,
                Optional.of(
                    ImmutableMap.of("pageToken", firstPage.nextPageToken(), "pageSize", "2")))
            .get();
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), secondPageResponse.getStatus());
    ListTablesResponse secondPage = secondPageResponse.readEntity(ListTablesResponse.class);
    Assertions.assertEquals(1, secondPage.identifiers().size());
    Assertions.assertNull(secondPage.nextPageToken());

    // Verify combined results
    Set<String> paginatedNames =
        java.util.stream.Stream.concat(
                firstPage.identifiers().stream(), secondPage.identifiers().stream())
            .map(id -> id.name())
            .collect(Collectors.toSet());
    Assertions.assertEquals(ImmutableSet.of("page_v1", "page_v2", "page_v3"), paginatedNames);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCreateView(Namespace namespace) {
    verifyCreateViewFail(namespace, "create_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergCreateViewPreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergCreateViewFailureEvent);

    verifyCreateNamespaceSucc(namespace);

    verifyCreateViewSucc(namespace, "create_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergCreateViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergCreateViewEvent);

    verifyCreateViewFail(namespace, "create_foo1", 409);
    verifyCreateViewFail(namespace, "", 400);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadView(Namespace namespace) {
    verifyLoadViewFail(namespace, "load_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergLoadViewFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "load_foo1");

    dummyEventListener.clearEvent();
    verifyLoadViewSucc(namespace, "load_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergLoadViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergLoadViewEvent);

    verifyLoadViewFail(namespace, "load_foo2", 404);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testReplaceView(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "replace_foo1");
    ViewMetadata metadata = getViewMeta(namespace, "replace_foo1");

    dummyEventListener.clearEvent();
    verifyReplaceSucc(namespace, "replace_foo1", metadata);
    PreEvent replacePreEvent = dummyEventListener.popPreEvent();
    Event replacePostEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(replacePreEvent instanceof IcebergReplaceViewPreEvent);
    Assertions.assertTrue(replacePostEvent instanceof IcebergReplaceViewEvent);
    IcebergReplaceViewEvent replaceViewEvent = (IcebergReplaceViewEvent) replacePostEvent;
    Assertions.assertNotNull(replaceViewEvent.replaceViewRequest());
    Assertions.assertEquals(OperationType.REPLACE_VIEW, replacePreEvent.operationType());
    Assertions.assertEquals(OperationType.REPLACE_VIEW, replacePostEvent.operationType());

    verifyDropViewSucc(namespace, "replace_foo1");

    dummyEventListener.clearEvent();
    verifyUpdateViewFail(namespace, "replace_foo1", 404, metadata);
    PreEvent replaceFailurePreEvent = dummyEventListener.popPreEvent();
    Event replaceFailurePostEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(replaceFailurePreEvent instanceof IcebergReplaceViewPreEvent);
    Assertions.assertTrue(replaceFailurePostEvent instanceof IcebergReplaceViewFailureEvent);
    Assertions.assertEquals(OperationType.REPLACE_VIEW, replaceFailurePreEvent.operationType());
    Assertions.assertEquals(OperationType.REPLACE_VIEW, replaceFailurePostEvent.operationType());

    verifyDropNamespaceSucc(namespace);
    verifyUpdateViewFail(namespace, "replace_foo1", 404, metadata);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testDropView(Namespace namespace) {
    verifyDropViewFail(namespace, "drop_foo1", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergDropViewFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyDropViewFail(namespace, "drop_foo1", 404);

    verifyCreateViewSucc(namespace, "drop_foo1");

    dummyEventListener.clearEvent();
    verifyDropViewSucc(namespace, "drop_foo1");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergDropViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergDropViewEvent);

    verifyLoadViewFail(namespace, "drop_foo1", 404);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testViewExits(Namespace namespace) {
    verifyViewExistsStatusCode(namespace, "exists_foo2", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergViewExistsPreEvent);
    Event postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergViewExistsEvent);
    Assertions.assertEquals(false, ((IcebergViewExistsEvent) postEvent).isExists());

    verifyCreateNamespaceSucc(namespace);
    verifyViewExistsStatusCode(namespace, "exists_foo2", 404);

    verifyCreateViewSucc(namespace, "exists_foo1");
    dummyEventListener.clearEvent();
    verifyViewExistsStatusCode(namespace, "exists_foo1", 204);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergViewExistsPreEvent);
    postEvent = dummyEventListener.popPostEvent();
    Assertions.assertTrue(postEvent instanceof IcebergViewExistsEvent);
    Assertions.assertEquals(true, ((IcebergViewExistsEvent) postEvent).isExists());

    verifyLoadViewSucc(namespace, "exists_foo1");
  }

  @ParameterizedTest
  @MethodSource(
      "org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testPrefixesAndNamespaces")
  void testRenameTable(String prefix, Namespace namespace) {
    setUrlPathWithPrefix(prefix);
    // namespace not exits
    verifyRenameViewFail(namespace, "rename_foo1", "rename_foo3", 404);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRenameViewPreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergRenameViewFailureEvent);

    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "rename_foo1");

    dummyEventListener.clearEvent();
    // rename
    verifyRenameViewSucc(namespace, "rename_foo1", "rename_foo2");
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergRenameViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergRenameViewEvent);

    verifyLoadViewFail(namespace, "rename_foo1", 404);
    verifyLoadViewSucc(namespace, "rename_foo2");

    // source view not exists
    verifyRenameViewFail(namespace, "rename_foo1", "rename_foo3", 404);

    // dest view exists
    verifyCreateViewSucc(namespace, "rename_foo3");
    verifyRenameViewFail(namespace, "rename_foo2", "rename_foo3", 409);
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testViewOperationsWithEncodedName(Namespace namespace) {
    // View names with special characters that require percent-encoding in URL paths.
    // RESTUtil.encodeString encodes them for the URL, and the server should decode
    // them back via RESTUtil.decodeString thanks to @Encoded on the path parameter.
    String[] specialNames = {"view.with.dots", "view@special"};

    verifyCreateNamespaceSucc(namespace);

    for (String originalName : specialNames) {
      String encodedName = RESTUtil.encodeString(originalName);

      // Create uses the original name in the request body, not in the URL path
      verifyCreateViewSucc(namespace, originalName);

      // Load, exists use the encoded name in the URL path
      verifyLoadViewSucc(namespace, encodedName);
      verifyViewExistsStatusCode(namespace, encodedName, 204);

      // Replace: load metadata with encoded path, then replace with encoded path
      ViewMetadata metadata = getViewMeta(namespace, encodedName);
      verifyReplaceSucc(namespace, encodedName, metadata);
    }

    // Verify list returns the original (decoded) view names
    verifyLisViewSucc(namespace, ImmutableSet.copyOf(specialNames));

    for (String originalName : specialNames) {
      verifyDropViewSucc(namespace, RESTUtil.encodeString(originalName));
    }
    verifyDropNamespaceSucc(namespace);
  }

  private Response doCreateView(Namespace ns, String name) {
    CreateViewRequest createViewRequest =
        ImmutableCreateViewRequest.builder()
            .name(name)
            .schema(viewSchema)
            .viewVersion(
                ImmutableViewVersion.builder()
                    .versionId(1)
                    .timestampMillis(System.currentTimeMillis())
                    .schemaId(1)
                    .defaultNamespace(IcebergRestTestUtil.TEST_NAMESPACE_NAME)
                    .addRepresentations(
                        ImmutableSQLViewRepresentation.builder()
                            .sql(VIEW_QUERY)
                            .dialect("spark")
                            .build())
                    .build())
            .build();
    return getViewClientBuilder(ns)
        .post(Entity.entity(createViewRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doLoadView(Namespace ns, String name) {
    return getViewClientBuilder(ns, Optional.of(name)).get();
  }

  private void verifyLoadViewSucc(Namespace ns, String name) {
    Response response = doLoadView(ns, name);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());

    LoadViewResponse loadViewResponse = response.readEntity(LoadViewResponse.class);
    Assertions.assertEquals(viewSchema.columns(), loadViewResponse.metadata().schema().columns());
  }

  private void verifyCreateViewFail(Namespace ns, String name, int status) {
    Response response = doCreateView(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyCreateViewSucc(Namespace ns, String name) {
    Response response = doCreateView(ns, name);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    LoadViewResponse loadViewResponse = response.readEntity(LoadViewResponse.class);
    Schema schema = loadViewResponse.metadata().schema();
    Assertions.assertEquals(schema.columns(), viewSchema.columns());
  }

  private void verifyLoadViewFail(Namespace ns, String name, int status) {
    Response response = doLoadView(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyReplaceSucc(Namespace ns, String name, ViewMetadata base) {
    Response response = doReplaceView(ns, name, base);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    LoadViewResponse loadViewResponse = response.readEntity(LoadViewResponse.class);
    Assertions.assertEquals(
        newViewSchema.columns(), loadViewResponse.metadata().schema().columns());
  }

  private Response doReplaceView(Namespace ns, String name, ViewMetadata base) {
    ViewMetadata.Builder builder =
        ViewMetadata.buildFrom(base).setCurrentVersion(base.currentVersion(), newViewSchema);
    ViewMetadata replacement = builder.build();
    UpdateTableRequest updateTableRequest =
        UpdateTableRequest.create(
            null,
            UpdateRequirements.forReplaceView(base, replacement.changes()),
            replacement.changes());
    return getViewClientBuilder(ns, Optional.of(name))
        .post(Entity.entity(updateTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private ViewMetadata getViewMeta(Namespace ns, String viewName) {
    Response response = doLoadView(ns, viewName);
    LoadViewResponse loadViewResponse = response.readEntity(LoadViewResponse.class);
    return loadViewResponse.metadata();
  }

  private void verifyUpdateViewFail(Namespace ns, String name, int status, ViewMetadata base) {
    Response response = doReplaceView(ns, name, base);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyDropViewSucc(Namespace ns, String name) {
    Response response = doDropView(ns, name);
    Assertions.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  private Response doDropView(Namespace ns, String name) {
    return getViewClientBuilder(ns, Optional.of(name)).delete();
  }

  private void verifyDropViewFail(Namespace ns, String name, int status) {
    Response response = doDropView(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyViewExistsStatusCode(Namespace ns, String name, int status) {
    Response response = doViewExists(ns, name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private Response doViewExists(Namespace ns, String name) {
    return getViewClientBuilder(ns, Optional.of(name)).head();
  }

  private void verifyListViewFail(Namespace ns, int status) {
    Response response = doListView(ns);
    Assertions.assertEquals(status, response.getStatus());
  }

  private Response doListView(Namespace ns) {
    return getViewClientBuilder(ns).get();
  }

  private void verifyLisViewSucc(Namespace ns, Set<String> expectedTableNames) {
    Response response = doListView(ns);
    Assertions.assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    ListTablesResponse listTablesResponse = response.readEntity(ListTablesResponse.class);
    Set<String> tableNames =
        listTablesResponse.identifiers().stream()
            .map(identifier -> identifier.name())
            .collect(Collectors.toSet());
    Assertions.assertEquals(expectedTableNames, tableNames);
  }

  private void verifyRenameViewFail(Namespace ns, String source, String dest, int status) {
    Response response = doRenameView(ns, source, dest);
    Assertions.assertEquals(status, response.getStatus());
  }

  private Response doRenameView(Namespace ns, String source, String dest) {
    RenameTableRequest renameTableRequest =
        RenameTableRequest.builder()
            .withSource(TableIdentifier.of(ns, source))
            .withDestination(TableIdentifier.of(ns, dest))
            .build();
    return getRenameViewClientBuilder()
        .post(Entity.entity(renameTableRequest, MediaType.APPLICATION_JSON_TYPE));
  }

  private void verifyRenameViewSucc(Namespace ns, String source, String dest) {
    Response response = doRenameView(ns, source, dest);
    Assertions.assertEquals(Response.Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  @Test
  @SuppressWarnings("deprecation")
  void testIcebergListViewEventDeprecatedConstructorReturnsNegativeCount() {
    IcebergListViewEvent event =
        new IcebergListViewEvent(
            Mockito.mock(IcebergRequestContext.class),
            NameIdentifier.of("metalake", "catalog", "schema"));
    Assertions.assertEquals(-1, event.resultCount());
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadViewReturnsETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "etag_foo1");

    Response response = doLoadView(namespace, "etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present in load view response");
    Assertions.assertFalse(etag.isEmpty(), "ETag header should not be empty");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadViewReturns304WhenETagMatches(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "etag_304_foo1");

    // First, load the view to get the ETag
    Response firstResponse = doLoadView(namespace, "etag_304_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), firstResponse.getStatus());
    String etag = firstResponse.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present");

    // Second, load the view with If-None-Match header set to the ETag
    Response secondResponse =
        getViewClientBuilder(namespace, Optional.of("etag_304_foo1"))
            .header(IcebergViewOperations.IF_NONE_MATCH, etag)
            .get();
    Assertions.assertEquals(
        Status.NOT_MODIFIED.getStatusCode(),
        secondResponse.getStatus(),
        "Should return 304 when ETag matches");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadViewReturns200WhenETagDoesNotMatch(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "etag_mismatch_foo1");

    // Load with a non-matching If-None-Match header
    Response response =
        getViewClientBuilder(namespace, Optional.of("etag_mismatch_foo1"))
            .header(IcebergViewOperations.IF_NONE_MATCH, "\"non-matching-etag-value\"")
            .get();
    Assertions.assertEquals(
        Status.OK.getStatusCode(),
        response.getStatus(),
        "Should return 200 when ETag does not match");
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadViewETagConsistency(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "etag_consistent_foo1");

    // Load the view twice and verify the ETag is the same
    Response firstResponse = doLoadView(namespace, "etag_consistent_foo1");
    String firstEtag = firstResponse.getHeaderString("ETag");

    Response secondResponse = doLoadView(namespace, "etag_consistent_foo1");
    String secondEtag = secondResponse.getHeaderString("ETag");

    Assertions.assertEquals(
        firstEtag, secondEtag, "ETag should be consistent for the same view metadata");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testLoadViewETagChangesAfterReplace(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "etag_replace_foo1");

    // Load the view and get the initial ETag
    Response firstResponse = doLoadView(namespace, "etag_replace_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), firstResponse.getStatus());
    String firstEtag = firstResponse.getHeaderString("ETag");
    Assertions.assertNotNull(firstEtag, "ETag header should be present");

    // Replace the view
    ViewMetadata metadata = getViewMeta(namespace, "etag_replace_foo1");
    verifyReplaceSucc(namespace, "etag_replace_foo1", metadata);

    // Load the view again and verify the ETag has changed
    Response secondResponse = doLoadView(namespace, "etag_replace_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), secondResponse.getStatus());
    String secondEtag = secondResponse.getHeaderString("ETag");
    Assertions.assertNotNull(secondEtag, "ETag header should be present after replace");
    Assertions.assertNotEquals(
        firstEtag, secondEtag, "ETag should change after view metadata is updated");

    // Verify old ETag no longer returns 304
    Response thirdResponse =
        getViewClientBuilder(namespace, Optional.of("etag_replace_foo1"))
            .header(IcebergViewOperations.IF_NONE_MATCH, firstEtag)
            .get();
    Assertions.assertEquals(
        Status.OK.getStatusCode(),
        thirdResponse.getStatus(),
        "Old ETag should not return 304 after view update");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCreateViewReturnsETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    Response response = doCreateView(namespace, "create_etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present in create view response");
    Assertions.assertFalse(etag.isEmpty(), "ETag header should not be empty");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testCreateViewETagMatchesLoadViewETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    Response createResponse = doCreateView(namespace, "create_load_etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), createResponse.getStatus());
    String createEtag = createResponse.getHeaderString("ETag");
    Assertions.assertNotNull(createEtag, "ETag should be present in create response");

    // Load the same view — ETag should match
    Response loadResponse = doLoadView(namespace, "create_load_etag_foo1");
    Assertions.assertEquals(Status.OK.getStatusCode(), loadResponse.getStatus());
    String loadEtag = loadResponse.getHeaderString("ETag");
    Assertions.assertNotNull(loadEtag, "ETag should be present in load response");

    Assertions.assertEquals(
        createEtag, loadEtag, "ETag from createView should match ETag from loadView");

    // The create ETag should be reusable for If-None-Match on a subsequent loadView
    Response conditionalResponse =
        getViewClientBuilder(namespace, Optional.of("create_load_etag_foo1"))
            .header(IcebergViewOperations.IF_NONE_MATCH, createEtag)
            .get();
    Assertions.assertEquals(
        Status.NOT_MODIFIED.getStatusCode(),
        conditionalResponse.getStatus(),
        "Create ETag should produce 304 on subsequent unchanged loadView");
  }

  @ParameterizedTest
  @MethodSource("org.apache.gravitino.iceberg.service.rest.IcebergRestTestUtil#testNamespaces")
  void testReplaceViewReturnsETag(Namespace namespace) {
    verifyCreateNamespaceSucc(namespace);
    verifyCreateViewSucc(namespace, "replace_etag_foo1");
    ViewMetadata metadata = getViewMeta(namespace, "replace_etag_foo1");
    Response response = doReplaceView(namespace, "replace_etag_foo1", metadata);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    String etag = response.getHeaderString("ETag");
    Assertions.assertNotNull(etag, "ETag header should be present in replace view response");
    Assertions.assertFalse(etag.isEmpty(), "ETag header should not be empty");
  }
}
