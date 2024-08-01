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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestIcebergNamespaceOperations extends IcebergTestBase {

  private final Map<String, String> properties = ImmutableMap.of("a", "b");
  private final Map<String, String> updatedProperties = ImmutableMap.of("b", "c");

  @Override
  protected Application configure() {
    return IcebergRestTestUtil.getIcebergResourceConfig(IcebergNamespaceOperations.class);
  }

  private Response doCreateNamespace(String... name) {
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder()
            .withNamespace(Namespace.of(name))
            .setProperties(properties)
            .build();
    return getNamespaceClientBuilder()
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doRegisterTable(String tableName) {
    RegisterTableRequest request =
        ImmutableRegisterTableRequest.builder().name(tableName).metadataLocation("mock").build();
    return getNamespaceClientBuilder(
            Optional.of("register_ns"), Optional.of("register"), Optional.empty())
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doListNamespace(Optional<String> parent) {
    Optional<Map<String, String>> queryParam =
        parent.isPresent()
            ? Optional.of(ImmutableMap.of("parent", parent.get()))
            : Optional.empty();
    return getNamespaceClientBuilder(Optional.empty(), Optional.empty(), queryParam).get();
  }

  private Response doUpdateNamespace(String name) {
    UpdateNamespacePropertiesRequest request =
        UpdateNamespacePropertiesRequest.builder()
            .removeAll(Arrays.asList("a", "a1"))
            .updateAll(updatedProperties)
            .build();
    return getUpdateNamespaceClientBuilder(name)
        .post(Entity.entity(request, MediaType.APPLICATION_JSON_TYPE));
  }

  private Response doLoadNamespace(String name) {
    return getNamespaceClientBuilder(Optional.of(name)).get();
  }

  private Response doDropNamespace(String name) {
    return getNamespaceClientBuilder(Optional.of(name)).delete();
  }

  private void verifyLoadNamespaceFail(int status, String name) {
    Response response = doLoadNamespace(name);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyLoadNamespaceSucc(String name) {
    Response response = doLoadNamespace(name);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    GetNamespaceResponse r = response.readEntity(GetNamespaceResponse.class);
    Assertions.assertEquals(name, r.namespace().toString());
    Assertions.assertEquals(properties, r.properties());
  }

  protected void verifyDropNamespaceSucc(String name) {
    Response response = doDropNamespace(name);
    Assertions.assertEquals(Status.NO_CONTENT.getStatusCode(), response.getStatus());
  }

  private void verifyDropNamespaceFail(int status, String name) {
    Response response = doDropNamespace(name);
    Assertions.assertEquals(status, response.getStatus());
  }

  protected void verifyCreateNamespaceSucc(String... name) {
    Response response = doCreateNamespace(name);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    CreateNamespaceResponse namespaceResponse = response.readEntity(CreateNamespaceResponse.class);
    Assertions.assertTrue(namespaceResponse.namespace().equals(Namespace.of(name)));

    Assertions.assertEquals(namespaceResponse.properties(), properties);
  }

  private void verifyRegisterTableSucc(String tableName) {
    Response response = doRegisterTable(tableName);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }

  private void verifyRegisterTableFail(int statusCode, String tableName) {
    Response response = doRegisterTable(tableName);
    Assertions.assertEquals(statusCode, response.getStatus());
  }

  private void verifyCreateNamespaceFail(int statusCode, String... name) {
    Response response = doCreateNamespace(name);
    Assertions.assertEquals(statusCode, response.getStatus());
  }

  @Test
  void testCreateNamespace() {
    verifyCreateNamespaceSucc("create_foo1");

    // Already Exists Exception
    verifyCreateNamespaceFail(409, "create_foo1");

    // multi level namespaces
    verifyCreateNamespaceSucc("create_foo2", "create_foo3");

    verifyCreateNamespaceFail(400, "");
  }

  @Test
  void testLoadNamespace() {
    verifyCreateNamespaceSucc("load_foo1");
    verifyLoadNamespaceSucc("load_foo1");
    // load a schema not exists
    verifyLoadNamespaceFail(404, "load_foo2");
  }

  @Test
  void testDropNamespace() {
    verifyCreateNamespaceSucc("drop_foo1");
    verifyDropNamespaceSucc("drop_foo1");
    verifyLoadNamespaceFail(404, "drop_foo1");

    // drop fail, no such namespace
    verifyDropNamespaceFail(404, "drop_foo2");
    // jersery route failed
    verifyDropNamespaceFail(500, "");
  }

  @Test
  void testRegisterTable() {
    verifyRegisterTableSucc("register_foo1");
    // Iceberg REST service will throw AlreadyExistsException in test if table name contains 'fail'
    verifyRegisterTableFail(409, "fail_register_foo1");
  }

  private void dropAllExistingNamespace() {
    Response response = doListNamespace(Optional.empty());
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListNamespacesResponse r = response.readEntity(ListNamespacesResponse.class);
    r.namespaces().forEach(n -> doDropNamespace(n.toString()));
  }

  private void verifyListNamespaceFail(Optional<String> parent, int status) {
    Response response = doListNamespace(parent);
    Assertions.assertEquals(status, response.getStatus());
  }

  private void verifyListNamespaceSucc(Optional<String> parent, List<String> schemas) {
    Response response = doListNamespace(parent);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    ListNamespacesResponse r = response.readEntity(ListNamespacesResponse.class);
    List<String> ns = r.namespaces().stream().map(n -> n.toString()).collect(Collectors.toList());
    Assertions.assertEquals(schemas, ns);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testListNamespace(boolean withPrefix) {
    setUrlPathWithPrefix(withPrefix);
    dropAllExistingNamespace();
    verifyListNamespaceSucc(Optional.empty(), Arrays.asList());

    doCreateNamespace("list_foo1");
    doCreateNamespace("list_foo2");
    doCreateNamespace("list_foo3", "a");
    doCreateNamespace("list_foo3", "b");

    verifyListNamespaceSucc(Optional.empty(), Arrays.asList("list_foo1", "list_foo2", "list_foo3"));
    verifyListNamespaceSucc(Optional.of("list_foo3"), Arrays.asList("list_foo3.a", "list_foo3.b"));

    verifyListNamespaceFail(Optional.of("list_fooxx"), 404);
  }

  private void verifyUpdateNamespaceSucc(String name) {
    Response response = doUpdateNamespace(name);
    Assertions.assertEquals(Status.OK.getStatusCode(), response.getStatus());

    UpdateNamespacePropertiesResponse r =
        response.readEntity(UpdateNamespacePropertiesResponse.class);
    Assertions.assertEquals(Arrays.asList("a"), r.removed());
    Assertions.assertEquals(Arrays.asList("a1"), r.missing());
    Assertions.assertEquals(Arrays.asList("b"), r.updated());
  }

  private void verifyUpdateNamespaceFail(int status, String name) {
    Response response = doUpdateNamespace(name);
    Assertions.assertEquals(status, response.getStatus());
  }

  @Test
  void testUpdateNamespace() {
    verifyCreateNamespaceSucc("update_foo1");
    verifyUpdateNamespaceSucc("update_foo1");

    verifyUpdateNamespaceFail(404, "update_foo2");
  }
}
