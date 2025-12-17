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

import static org.apache.gravitino.dto.util.DTOConverters.toDTO;
import static org.apache.gravitino.dto.util.DTOConverters.toDTOs;
import static org.apache.gravitino.dto.util.DTOConverters.toFunctionArg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.AuditDTO;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.function.FunctionColumnDTO;
import org.apache.gravitino.dto.function.FunctionImplDTO;
import org.apache.gravitino.dto.function.FunctionParamDTO;
import org.apache.gravitino.dto.function.FunctionSignatureDTO;
import org.apache.gravitino.dto.requests.FunctionDeleteRequest;
import org.apache.gravitino.dto.requests.FunctionRegisterRequest;
import org.apache.gravitino.dto.requests.FunctionUpdateRequest;
import org.apache.gravitino.dto.requests.FunctionUpdatesRequest;
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.requests.TableUpdateRequest;
import org.apache.gravitino.dto.requests.TableUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.FunctionListResponse;
import org.apache.gravitino.dto.responses.FunctionResponse;
import org.apache.gravitino.dto.responses.TableResponse;
import org.apache.gravitino.exceptions.FunctionAlreadyExistsException;
import org.apache.gravitino.exceptions.NoSuchFunctionException;
import org.apache.gravitino.exceptions.NoSuchFunctionVersionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.function.Function;
import org.apache.gravitino.function.FunctionCatalog;
import org.apache.gravitino.function.FunctionChange;
import org.apache.gravitino.function.FunctionColumn;
import org.apache.gravitino.function.FunctionImpl;
import org.apache.gravitino.function.FunctionParam;
import org.apache.gravitino.function.FunctionSignature;
import org.apache.gravitino.function.FunctionType;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rest.RESTUtils;

/**
 * Relational catalog is a catalog implementation that supports relational database like metadata
 * operations, for example, schemas and tables list, creation, update and deletion. A Relational
 * catalog is under the metalake.
 */
class RelationalCatalog extends BaseSchemaCatalog implements TableCatalog, FunctionCatalog {

  RelationalCatalog(
      Namespace namespace,
      String name,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties,
      AuditDTO auditDTO,
      RESTClient restClient) {
    super(namespace, name, type, provider, comment, properties, auditDTO, restClient);
  }

  @Override
  public TableCatalog asTableCatalog() {
    return this;
  }

  @Override
  public FunctionCatalog asFunctionCatalog() {
    return this;
  }

  /**
   * List all the tables under the given Schema namespace.
   *
   * @param namespace The namespace to list the tables under it. This namespace should have 1 level,
   *     which is the schema name;
   * @return A list of {@link NameIdentifier} of the tables under the given namespace.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    checkTableNamespace(namespace);

    Namespace fullNamespace = getTableFullNamespace(namespace);
    EntityListResponse resp =
        restClient.get(
            formatTableRequestPath(fullNamespace),
            EntityListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return Arrays.stream(resp.identifiers())
        .map(ident -> NameIdentifier.of(ident.namespace().level(2), ident.name()))
        .toArray(NameIdentifier[]::new);
  }

  /**
   * Load the table with specified identifier.
   *
   * @param ident The identifier of the table to load, which should be "schema.table" format.
   * @return The {@link Table} with specified identifier.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    checkTableNameIdentifier(ident);

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.get(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(fullNamespace, resp.getTable(), restClient);
  }

  /**
   * Create a new table with specified identifier, columns, comment and properties.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @param columns The columns of the table.
   * @param comment The comment of the table.
   * @param properties The properties of the table.
   * @param partitioning The partitioning of the table.
   * @param indexes The indexes of the table.
   * @return The created {@link Table}.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws TableAlreadyExistsException if the table with specified identifier already exists.
   */
  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitioning,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    checkTableNameIdentifier(ident);

    TableCreateRequest req =
        new TableCreateRequest(
            ident.name(),
            comment,
            toDTOs(columns),
            properties,
            toDTOs(sortOrders),
            toDTO(distribution),
            toDTOs(partitioning),
            toDTOs(indexes));
    req.validate();

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.post(
            formatTableRequestPath(fullNamespace),
            req,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(fullNamespace, resp.getTable(), restClient);
  }

  /**
   * Alter the table with specified identifier by applying the changes.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @param changes Table changes to apply to the table.
   * @return The altered {@link Table}.
   * @throws NoSuchTableException if the table with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    checkTableNameIdentifier(ident);

    List<TableUpdateRequest> reqs =
        Arrays.stream(changes)
            .map(DTOConverters::toTableUpdateRequest)
            .collect(Collectors.toList());
    TableUpdatesRequest updatesRequest = new TableUpdatesRequest(reqs);
    updatesRequest.validate();

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    TableResponse resp =
        restClient.put(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            updatesRequest,
            TableResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();

    return RelationalTable.from(fullNamespace, resp.getTable(), restClient);
  }

  /**
   * Drop the table with specified identifier.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @return true if the table is dropped successfully, false if the table does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    checkTableNameIdentifier(ident);

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    DropResponse resp =
        restClient.delete(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  /**
   * Purge the table with specified identifier.
   *
   * @param ident The identifier of the table, which should be "schema.table" format.
   * @return true if the table is purged successfully, false if the table does not exist.
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    checkTableNameIdentifier(ident);

    Namespace fullNamespace = getTableFullNamespace(ident.namespace());
    Map<String, String> params = new HashMap<>();
    params.put("purge", "true");
    DropResponse resp =
        restClient.delete(
            formatTableRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            params,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.tableErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  /**
   * List all the functions under the given schema namespace.
   *
   * @param namespace The namespace to list the functions under it. This namespace should have 1
   *     level, which is the schema name;
   * @return A list of {@link NameIdentifier} of the functions under the given namespace.
   * @throws NoSuchSchemaException if the schema with a specified namespace does not exist.
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
   * Load the functions with a specified identifier.
   *
   * @param ident The identifier of the functions to load, which should be "schema.function" format.
   * @return The functions with specified identifier.
   * @throws NoSuchFunctionException if the function with a specified identifier does not exist.
   */
  @Override
  public Function[] getFunction(NameIdentifier ident) throws NoSuchFunctionException {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionListResponse resp =
        restClient.get(
            formatFunctionRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            FunctionListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunctions();
  }

  /**
   * Load the functions with a specified identifier and version.
   *
   * @param ident The identifier of the functions to load, which should be "schema.function" format.
   * @param version The version to load.
   * @return The functions with specified identifier and version.
   * @throws NoSuchFunctionException if the function with a specified identifier does not exist.
   * @throws NoSuchFunctionVersionException if the function version does not exist.
   */
  @Override
  public Function[] getFunction(NameIdentifier ident, int version)
      throws NoSuchFunctionException, NoSuchFunctionVersionException {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    Map<String, String> params = new HashMap<>();
    params.put("version", String.valueOf(version));
    FunctionListResponse resp =
        restClient.get(
            formatFunctionRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            params,
            FunctionListResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunctions();
  }

  /**
   * Register a scalar or aggregate function.
   *
   * @param ident The identifier of the function, which should be "schema.function" format.
   * @param comment The comment of the function.
   * @param functionType The function type.
   * @param deterministic Whether the function is deterministic.
   * @param functionParams The parameters of the function.
   * @param returnType The return type of the function.
   * @param functionImpls The implementations of the function.
   * @return The created {@link Function}.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws FunctionAlreadyExistsException if the function with specified identifier already
   *     exists.
   */
  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      FunctionType functionType,
      boolean deterministic,
      FunctionParam[] functionParams,
      org.apache.gravitino.rel.types.Type returnType,
      FunctionImpl[] functionImpls)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    checkFunctionNameIdentifier(ident);

    FunctionRegisterRequest request =
        new FunctionRegisterRequest(
            new FunctionSignatureDTO(ident.name(), toFunctionParamDTOs(functionParams)),
            comment,
            functionType,
            deterministic,
            returnType,
            null,
            toFunctionImplDTOs(functionImpls));
    request.validate();

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionResponse resp =
        restClient.post(
            formatFunctionRequestPath(fullNamespace),
            request,
            FunctionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunction();
  }

  /**
   * Register a table-valued function.
   *
   * @param ident The identifier of the function, which should be "schema.function" format.
   * @param comment The comment of the function.
   * @param deterministic Whether the function is deterministic.
   * @param functionParams The parameters of the function.
   * @param returnColumns The return columns of the function.
   * @param functionImpls The implementations of the function.
   * @return The created {@link Function}.
   * @throws NoSuchSchemaException if the schema with specified namespace does not exist.
   * @throws FunctionAlreadyExistsException if the function with specified identifier already
   *     exists.
   */
  @Override
  public Function registerFunction(
      NameIdentifier ident,
      String comment,
      boolean deterministic,
      FunctionParam[] functionParams,
      FunctionColumn[] returnColumns,
      FunctionImpl[] functionImpls)
      throws NoSuchSchemaException, FunctionAlreadyExistsException {
    checkFunctionNameIdentifier(ident);

    FunctionRegisterRequest request =
        new FunctionRegisterRequest(
            new FunctionSignatureDTO(ident.name(), toFunctionParamDTOs(functionParams)),
            comment,
            FunctionType.TABLE,
            deterministic,
            null,
            toFunctionColumnDTOs(returnColumns),
            toFunctionImplDTOs(functionImpls));
    request.validate();

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionResponse resp =
        restClient.post(
            formatFunctionRequestPath(fullNamespace),
            request,
            FunctionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunction();
  }

  /**
   * Alter the function with a specified identifier by applying the changes.
   *
   * @param ident The identifier of the function, which should be "schema.function" format.
   * @param changes Function changes to apply to the function.
   * @return The altered {@link Function}.
   * @throws NoSuchFunctionException if the function with specified identifier does not exist.
   * @throws IllegalArgumentException if the changes are invalid.
   */
  @Override
  public Function alterFunction(NameIdentifier ident, FunctionChange... changes)
      throws NoSuchFunctionException, IllegalArgumentException {
    checkFunctionNameIdentifier(ident);

    List<FunctionUpdateRequest> requests =
        Arrays.stream(changes)
            .map(RelationalCatalog::toFunctionUpdateRequest)
            .collect(Collectors.toList());
    FunctionUpdatesRequest updatesRequest = new FunctionUpdatesRequest(requests);
    updatesRequest.validate();

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionResponse resp =
        restClient.put(
            formatFunctionRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            updatesRequest,
            FunctionResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();

    return resp.getFunction();
  }

  /**
   * Drop the function with specified identifier.
   *
   * @param ident The identifier of the function, which should be "schema.function" format.
   * @return true if the function is dropped successfully, false if the function does not exist.
   */
  @Override
  public boolean deleteFunction(NameIdentifier ident) {
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

  /**
   * Drop the function with a specified identifier and signature.
   *
   * @param ident The identifier of the function, which should be "schema.function" format.
   * @param signature The signature of the function.
   * @return true if the function is dropped successfully, false if the function does not exist.
   */
  @Override
  public boolean deleteFunction(NameIdentifier ident, FunctionSignature signature) {
    checkFunctionNameIdentifier(ident);

    Namespace fullNamespace = getFunctionFullNamespace(ident.namespace());
    FunctionDeleteRequest request = new FunctionDeleteRequest(toFunctionSignatureDTO(signature));
    request.validate();

    DropResponse resp =
        restClient.delete(
            formatFunctionRequestPath(fullNamespace) + "/" + RESTUtils.encodeString(ident.name()),
            request,
            DropResponse.class,
            Collections.emptyMap(),
            ErrorHandlers.functionErrorHandler());
    resp.validate();
    return resp.dropped();
  }

  @VisibleForTesting
  static String formatTableRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return new StringBuilder()
        .append(formatSchemaRequestPath(schemaNs))
        .append("/")
        .append(RESTUtils.encodeString(ns.level(2)))
        .append("/tables")
        .toString();
  }

  /**
   * Check whether the namespace of a table is valid, which should be "schema".
   *
   * @param namespace The namespace to check.
   */
  static void checkTableNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Table namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  /**
   * Check whether the NameIdentifier of a table is valid.
   *
   * @param ident The NameIdentifier to check, which should be "schema.table" format.
   */
  static void checkTableNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkTableNamespace(ident.namespace());
  }

  private static String formatFunctionRequestPath(Namespace ns) {
    Namespace schemaNs = Namespace.of(ns.level(0), ns.level(1));
    return formatSchemaRequestPath(schemaNs)
        + "/"
        + RESTUtils.encodeString(ns.level(2))
        + "/functions";
  }

  private static void checkFunctionNamespace(Namespace namespace) {
    Namespace.check(
        namespace != null && namespace.length() == 1,
        "Function namespace must be non-null and have 1 level, the input namespace is %s",
        namespace);
  }

  private static void checkFunctionNameIdentifier(NameIdentifier ident) {
    NameIdentifier.check(ident != null, "NameIdentifier must not be null");
    NameIdentifier.check(
        ident.name() != null && !ident.name().isEmpty(), "NameIdentifier name must not be empty");
    checkFunctionNamespace(ident.namespace());
  }

  private Namespace getFunctionFullNamespace(Namespace functionNamespace) {
    return Namespace.of(this.catalogNamespace().level(0), this.name(), functionNamespace.level(0));
  }

  private static FunctionParamDTO[] toFunctionParamDTOs(FunctionParam[] params) {
    if (params == null) {
      return null;
    }
    return Arrays.stream(params)
        .map(
            param ->
                new FunctionParamDTO(
                    param.name(),
                    param.dataType(),
                    param.comment(),
                    param.defaultValue() == null
                            || Column.DEFAULT_VALUE_NOT_SET.equals(param.defaultValue())
                        ? Column.DEFAULT_VALUE_NOT_SET
                        : toFunctionArg(param.defaultValue())))
        .toArray(FunctionParamDTO[]::new);
  }

  private static FunctionImplDTO[] toFunctionImplDTOs(FunctionImpl[] impls) {
    if (impls == null) {
      return new FunctionImplDTO[0];
    }
    return Arrays.stream(impls).map(FunctionImplDTO::from).toArray(FunctionImplDTO[]::new);
  }

  private static FunctionColumnDTO[] toFunctionColumnDTOs(FunctionColumn[] columns) {
    if (columns == null) {
      return new FunctionColumnDTO[0];
    }
    return Arrays.stream(columns)
        .map(column -> new FunctionColumnDTO(column.name(), column.dataType(), column.comment()))
        .toArray(FunctionColumnDTO[]::new);
  }

  private static FunctionSignatureDTO toFunctionSignatureDTO(FunctionSignature signature) {
    if (signature == null) {
      return null;
    }
    return new FunctionSignatureDTO(
        signature.name(), toFunctionParamDTOs(signature.functionParams()));
  }

  private static FunctionUpdateRequest toFunctionUpdateRequest(FunctionChange change) {
    if (change instanceof FunctionChange.UpdateComment) {
      FunctionChange.UpdateComment updateComment = (FunctionChange.UpdateComment) change;
      return new FunctionUpdateRequest.UpdateCommentRequest(
          toFunctionSignatureDTO(updateComment.signature()), updateComment.newComment());

    } else if (change instanceof FunctionChange.UpdateImplementations) {
      FunctionChange.UpdateImplementations updateImplementations =
          (FunctionChange.UpdateImplementations) change;
      return new FunctionUpdateRequest.UpdateImplementationsRequest(
          toFunctionSignatureDTO(updateImplementations.signature()),
          toFunctionImplDTOs(updateImplementations.newImplementations()));

    } else if (change instanceof FunctionChange.AddImplementation) {
      FunctionChange.AddImplementation addImplementation =
          (FunctionChange.AddImplementation) change;
      return new FunctionUpdateRequest.AddImplementationRequest(
          toFunctionSignatureDTO(addImplementation.signature()),
          FunctionImplDTO.from(addImplementation.implementation()));

    } else {
      throw new IllegalArgumentException(
          "Unknown change type: " + change.getClass().getSimpleName());
    }
  }

  /**
   * Get the full namespace of the table with the given table's short namespace (schema name).
   *
   * @param tableNamespace The table's short namespace, which is the schema name.
   * @return full namespace of the table, which is "metalake.catalog.schema" format.
   */
  private Namespace getTableFullNamespace(Namespace tableNamespace) {
    return Namespace.of(this.catalogNamespace().level(0), this.name(), tableNamespace.level(0));
  }

  /**
   * Create a new builder for the relational catalog.
   *
   * @return A new builder for the relational catalog.
   */
  public static Builder builder() {
    return new Builder();
  }

  static class Builder extends CatalogDTO.Builder<Builder> {
    /** The REST client to send the requests. */
    private RESTClient restClient;
    /** The namespace of the catalog */
    private Namespace namespace;

    protected Builder() {}

    Builder withNamespace(Namespace namespace) {
      this.namespace = namespace;
      return this;
    }

    Builder withRestClient(RESTClient restClient) {
      this.restClient = restClient;
      return this;
    }

    @Override
    public RelationalCatalog build() {
      Namespace.check(
          namespace != null && namespace.length() == 1,
          "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
          namespace);
      Preconditions.checkArgument(restClient != null, "restClient must be set");
      Preconditions.checkArgument(StringUtils.isNotBlank(name), "name must not be blank");
      Preconditions.checkArgument(type != null, "type must not be null");
      Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
      Preconditions.checkArgument(audit != null, "audit must not be null");

      return new RelationalCatalog(
          namespace, name, type, provider, comment, properties, audit, restClient);
    }
  }
}
