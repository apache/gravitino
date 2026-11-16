/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.server.web.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import javax.ws.rs.core.UriInfo;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.rest.IcebergNamespaceOperations;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

/**
 * Tests that authorization handlers answer a missing or incomplete request body with 400 instead of
 * a 403 or an internal error.
 */
public class TestIcebergRequestBodyAuthzHandlers {

  @Test
  public void testCreateNamespaceWithoutBodyOrNamespaceIsBadRequest() throws Exception {
    Parameter[] parameters = parameters("createNamespace", CreateNamespaceRequest.class);
    assertThrows(
        BadRequestException.class,
        () ->
            new CreateNamespaceAuthzHandler(parameters, new Object[] {null})
                .process(new HashMap<>()));

    CreateNamespaceRequest empty = Mockito.mock(CreateNamespaceRequest.class);
    Mockito.when(empty.namespace()).thenReturn(Namespace.empty());
    assertThrows(
        BadRequestException.class,
        () ->
            new CreateNamespaceAuthzHandler(parameters, new Object[] {empty})
                .process(new HashMap<>()));

    CreateNamespaceRequest noNamespace = Mockito.mock(CreateNamespaceRequest.class);
    assertThrows(
        BadRequestException.class,
        () ->
            new CreateNamespaceAuthzHandler(parameters, new Object[] {noNamespace})
                .process(new HashMap<>()));
  }

  @Test
  public void testRenameTableWithoutSourceOrDestinationIsBadRequest() throws Exception {
    Parameter[] parameters = parameters("renameTable", RenameTableRequest.class);
    for (RenameTableRequest request : incompleteRenames()) {
      assertThrows(
          BadRequestException.class,
          () ->
              new RenameTableAuthzHandler(parameters, new Object[] {request})
                  .process(new HashMap<>()));
    }
  }

  @ParameterizedTest
  @MethodSource("invalidNamespaceLevels")
  void testCreateRejectsDottedLevelsBeforeResolvingParent(Namespace namespace) throws Exception {
    Method method =
        IcebergNamespaceOperations.class.getMethod(
            "createNamespace",
            String.class,
            CreateNamespaceRequest.class,
            String.class,
            UriInfo.class);
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).build();
    NameIdentifier catalog = NameIdentifier.of("metalake", "catalog");
    Map<EntityType, NameIdentifier> identifiers = new HashMap<>();
    identifiers.put(EntityType.CATALOG, catalog);

    assertThrows(
        BadRequestException.class,
        () ->
            new CreateNamespaceAuthzHandler(
                    method.getParameters(), new Object[] {"catalog/", request, null, null})
                .process(identifiers));
    assertEquals(Map.of(EntityType.CATALOG, catalog), identifiers);
  }

  @Test
  public void testRenameViewWithoutSourceOrDestinationIsBadRequest() throws Exception {
    Parameter[] parameters = parameters("renameView", RenameTableRequest.class);
    for (RenameTableRequest request : incompleteRenames()) {
      assertThrows(
          BadRequestException.class,
          () ->
              new RenameViewAuthzHandler(parameters, new Object[] {request})
                  .process(new HashMap<>()));
    }
  }

  private static RenameTableRequest[] incompleteRenames() {
    TableIdentifier ident = TableIdentifier.of(Namespace.of("db"), "t");
    RenameTableRequest noDestination = Mockito.mock(RenameTableRequest.class);
    Mockito.when(noDestination.source()).thenReturn(ident);
    RenameTableRequest noSource = Mockito.mock(RenameTableRequest.class);
    Mockito.when(noSource.destination()).thenReturn(ident);
    return new RenameTableRequest[] {null, noDestination, noSource};
  }

  private static Parameter[] parameters(String methodName, Class<?> requestClass) throws Exception {
    Method method = TestOperations.class.getMethod(methodName, requestClass);
    return method.getParameters();
  }

  private static Stream<Namespace> invalidNamespaceLevels() {
    return Stream.of(
        Namespace.of("a.b"),
        Namespace.of("a.b", "child"),
        Namespace.of("parent", "a.b"),
        Namespace.of("parent", "a.b", "child"));
  }

  /** Provides annotated parameters like the REST operations do. */
  @SuppressWarnings("unused")
  public static class TestOperations {
    public void createNamespace(
        @IcebergAuthorizationMetadata(type = RequestType.CREATE_NAMESPACE)
            CreateNamespaceRequest request) {}

    public void renameTable(
        @IcebergAuthorizationMetadata(type = RequestType.RENAME_TABLE)
            RenameTableRequest request) {}

    public void renameView(
        @IcebergAuthorizationMetadata(type = RequestType.RENAME_VIEW) RenameTableRequest request) {}
  }
}
