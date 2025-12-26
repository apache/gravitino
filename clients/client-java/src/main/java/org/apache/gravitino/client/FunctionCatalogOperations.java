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

import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.function.FunctionColumnDTO;
import org.apache.gravitino.dto.function.FunctionDefinitionDTO;
import org.apache.gravitino.dto.requests.FunctionRegisterRequest;
import org.apache.gravitino.dto.requests.FunctionUpdateRequest;
import org.apache.gravitino.dto.requests.FunctionUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.FunctionListResponse;
import org.apache.gravitino.dto.responses.FunctionResponse;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionDefinition;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rest.RESTUtils;

/**
 * Function catalog operations helper class that provides implementations for function management.
 * This class is used by catalogs that support function operations (e.g., RelationalCatalog).
 */
class FunctionCatalogOperations implements FunctionCatalog {

  private final RESTClient restClient;
  private final Namespace catalogNamespace;
  private final String catalogName;

  FunctionCatalogOperations(RESTClient restClient, Namespace catalogNamespace, String catalogName) {
    this.restClient = restClient;
    this.catalogNamespace = catalogNamespace;
    this.catalogName = catalogName;
  }

  /**
   * List the functions in a schema namespace from the catalog.
   *
   * @param namespace A schema namespace. This namespace should have 1 level, which is the schema
   *     name;
   * @return An array of {@link NameIdentifier} of functions under the given namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public NameIdentifier[] listFunctions(Namespace namespace) throws NoSuchSchemaException {
    checkFunctionNamespace(namespace);

    Namespace fullNamespace = getFunctionFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatFunctionRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * List the functions with details in a schema namespace from the catalog.
   *
   * @param namespace A namespace.
   * @return An array of functions in the namespace.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public Function[] listFunctionInfos(Namespace namespace) throws NoSuchSchemaException {
    checkFunctionNamespace(namespace);

    Namespace fullNamespace = getFunctionFullNamespace(namespace);
    Map<String, String> params = new HashMap<>();
    params.put("details", "true");

    FunctionListResponse resp =
        restClient.get(
            formatFunctionRequestPath(fullNamespace),
            params,
            FunctionListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();
    return resp.getFunctions();
  }

  /**
   * Get a function by {@link NameIdentifier} from the catalog. This method returns the latest
   * version of the function.
   *
   * @param ident A function identifier, which should be "schema.function" format.
   * @return The function metadata.
   * @throws NoSuchFunctionException If the function does not exist.
   */
  @Override
  public Function getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionResponse resp =
        restClient.get(
            formatFunctionRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            FunctionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunction();
  }

  /**
   * Register a scalar or aggregate function with one or more definitions (overloads).
   *
   * @param ident The function identifier.
   * @param comment The optional function comment.
   * @param functionType The function type.
   * @param deterministic Whether the function is deterministic.
   * @param returnType The return type.
   * @param definitions The function definitions.
   * @return The registered function.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FunctionAlreadyExistsException If the function already exists.
   */
  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      Type returnType,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionRegisterRequest req =
        FunctionRegisterRequest.builder()
            .withName(ident.name())
            .withComment(comment)
            .withFunctionType(functionType)
            .withDeterministic(deterministic)
            .withReturnType(returnType)
            .withDefinitions(toFunctionDefinitionDTOs(definitions))
            .build();
    req.validate();

    FunctionResponse resp =
        restClient.post(
            formatFunctionRequestPath(fullNamespace),
            req,
            FunctionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunction();
  }

  /**
   * Register a table-valued function with one or more definitions (overloads).
   *
   * @param ident The function identifier.
   * @param comment The optional function comment.
   * @param deterministic Whether the function is deterministic.
   * @param returnColumns The return columns.
   * @param definitions The function definitions.
   * @return The registered function.
   * @throws NoSuchSchemaException If the schema does not exist.
   * @throws FunctionAlreadyExistsException If the function already exists.
   */
  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionColumn[] returnColumns,
      FunctionDefinition[] definitions)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionRegisterRequest req =
        FunctionRegisterRequest.builder()
            .withName(ident.name())
            .withComment(comment)
            .withFunctionType(FunctionType.TABLE)
            .withDeterministic(deterministic)
            .withReturnColumns(toFunctionColumnDTOs(returnColumns))
            .withDefinitions(toFunctionDefinitionDTOs(definitions))
            .build();
    req.validate();

    FunctionResponse resp =
        restClient.post(
            formatFunctionRequestPath(fullNamespace),
            req,
            FunctionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunction();
  }

  /**
   * Alter a function in the catalog.
   *
   * @param ident A function identifier, which should be "schema.function" format.
   * @param changes The changes to apply to the function.
   * @return The updated function metadata.
   * @throws NoSuchFunctionException If the function does not exist.
   * @throws IllegalArgumentException If the changes are invalid.
   */
  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    List<FunctionUpdateRequest> updates =
        Arrays.stream(changes)
            .map(DTOConverters::toFunctionUpdateRequest)
            .collect(Collectors.toList());
    FunctionUpdatesRequest req = new FunctionUpdatesRequest(updates);
    req.validate();

    FunctionResponse resp =
        restClient.put(
            formatFunctionRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            req,
            FunctionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunction();
  }

  /**
   * Drop a function from the catalog.
   *
   * @param ident A function identifier, which should be "schema.function" format.
   * @return true If the function is dropped, false the function did not exist.
   */
  @Override
  public boolean dropFunction(NameIdentifier ident) {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatFunctionRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.dropped();
  }

  @VisibleForTesting
  String formatFunctionRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(BaseSchemaCatalog.formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(RESTUtils.encodeString(ns.level(2)))
        .append("/functions")
        .toString();
  }

  /**
   * Check whether the namespace of a function is valid.
   *
   * @param namespace The namespace to check.
   */
  static void checkFunctionNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Function namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check whether the NameIdentifier of a function is valid.
   *
   * @param ident The NameIdentifier to check, which should be "schema.function" format.
   */
  static void checkFunctionNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkFunctionNamespace(ident.namespace());
  }

  /**
   * Get the full namespace of the function with the given function's short namespace (schema name).
   *
   * @param functionNamespace The function's short namespace, which is the schema name.
   * @return full namespace of the function, which is "metalake.catalog.schema" format.
   */
  private Namespace getFunctionFullNamespace(Namespace functionNamespace) {
    return Namespace.of(catalogNamespace.level(0), catalogName, functionNamespace.level(0));
  }

  private FunctionDefinitionDTO[] toFunctionDefinitionDTOs(FunctionDefinition[] definitions) {
    if (definitions == null) {
      return null;
    }
    return Arrays.stream(definitions)
        .map(DTOConverters::toFunctionDefinitionDTO)
        .toArray(FunctionDefinitionDTO[]::new);
  }

  private FunctionColumnDTO[] toFunctionColumnDTOs(FunctionColumn[] columns) {
    if (columns == null) {
      return null;
    }
    return Arrays.stream(columns)
        .map(DTOConverters::toFunctionColumnDTO)
        .toArray(FunctionColumnDTO[]::new);
  }
}
