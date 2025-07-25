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
import org.apache.gravitino.listener.api.event.IcebergViewExistsEvent;
import org.apache.gravitino.listener.api.event.IcebergViewExistsPreEvent;
import org.apache.iceberg.Schema;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
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
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergListViewEvent);
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
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergReplaceViewPreEvent);
    Assertions.assertTrue(dummyEventListener.popPostEvent() instanceof IcebergReplaceViewEvent);

    verifyDropViewSucc(namespace, "replace_foo1");

    dummyEventListener.clearEvent();
    verifyUpdateViewFail(namespace, "replace_foo1", 404, metadata);
    Assertions.assertTrue(dummyEventListener.popPreEvent() instanceof IcebergReplaceViewPreEvent);
    Assertions.assertTrue(
        dummyEventListener.popPostEvent() instanceof IcebergReplaceViewFailureEvent);

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
}
