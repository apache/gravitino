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
package org.apache.gravitino.lance.service.rest;

import static org.apache.gravitino.lance.common.ops.NamespaceWrapper.NAMESPACE_DELIMITER_DEFAULT;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_LOCATION;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_LOCATION_HEADER;
import static org.apache.gravitino.lance.common.utils.LanceConstants.LANCE_TABLE_PROPERTIES_PREFIX_HEADER;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.common.utils.LanceConstants;
import org.apache.gravitino.lance.common.utils.SerializationUtils;
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.AlterColumnsEntry;
import org.lance.namespace.model.AlterTableAlterColumnsRequest;
import org.lance.namespace.model.AlterTableAlterColumnsResponse;
import org.lance.namespace.model.AlterTableDropColumnsRequest;
import org.lance.namespace.model.AlterTableDropColumnsResponse;
import org.lance.namespace.model.CreateEmptyTableResponse;
import org.lance.namespace.model.CreateTableResponse;
import org.lance.namespace.model.DeclareTableRequest;
import org.lance.namespace.model.DeclareTableResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.RegisterTableRequest;
import org.lance.namespace.model.RegisterTableResponse;
import org.lance.namespace.model.TableExistsRequest;

@Path("/v1/table/{id}")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LanceTableOperations {

  private final NamespaceWrapper lanceNamespace;

  @Inject
  public LanceTableOperations(NamespaceWrapper lanceNamespace) {
    this.lanceNamespace = lanceNamespace;
  }

  @POST
  @Path("/describe")
  @Timed(name = "describe-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "describe-table", absolute = true)
  public Response describeTable(
      @PathParam("id") String tableId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      DescribeTableRequest request) {
    try {
      validateDescribeTableRequest(request);
      boolean vendCredentials =
          request.getVendCredentials() == null || Boolean.TRUE.equals(request.getVendCredentials());
      CredentialPrivilege privilege =
          vendCredentials ? getCredentialPrivilege(tableId, delimiter) : null;
      DescribeTableResponse response =
          lanceNamespace
              .asTableOps()
              .describeTable(
                  tableId, delimiter, Optional.ofNullable(request.getVersion()), privilege);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  private CredentialPrivilege getCredentialPrivilege(String tableId, String delimiter) {
    String[] parts = tableId.split(Pattern.quote(delimiter));
    if (parts.length != 3) {
      return CredentialPrivilege.READ;
    }

    String metalake = lanceNamespace.config().getGravitinoMetalake();
    NameIdentifier identifier = NameIdentifier.of(metalake, parts[0], parts[1], parts[2]);
    boolean writable =
        MetadataAuthzHelper.checkAccess(
            identifier,
            Entity.EntityType.TABLE,
            AuthorizationExpressionConstants.FILTER_MODIFY_TABLE_AUTHORIZATION_EXPRESSION);
    return writable ? CredentialPrivilege.WRITE : CredentialPrivilege.READ;
  }

  @POST
  @Path("/create")
  @Consumes("application/vnd.apache.arrow.stream")
  @Produces("application/json")
  @Timed(name = "create-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-table", absolute = true)
  public Response createTable(
      @PathParam("id") String tableId,
      @QueryParam("mode") @DefaultValue("create") String mode, // create, exist_ok, overwrite
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      @Context HttpHeaders headers,
      byte[] arrowStreamBody) {
    try {
      // Extract table properties from header
      MultivaluedMap<String, String> headersMap = headers.getRequestHeaders();
      String tableLocation = headersMap.getFirst(LANCE_TABLE_LOCATION_HEADER);
      String tableProperties = headersMap.getFirst(LANCE_TABLE_PROPERTIES_PREFIX_HEADER);
      Map<String, String> props = SerializationUtils.deserializeProperties(tableProperties);
      CreateTableResponse response =
          lanceNamespace
              .asTableOps()
              .createTable(
                  tableId,
                  normalizeCreateTableMode(mode),
                  delimiter,
                  tableLocation,
                  props,
                  arrowStreamBody);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  /**
   * According to the spec of lance-namespace with version 0.0.20 to 0.31, createEmptyTable only
   * stores the table metadata including its location, and will never touch lance storage.
   */
  @POST
  @Path("/create-empty")
  @Produces("application/json")
  @Timed(name = "create-empty-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-empty-table", absolute = true)
  @SuppressWarnings("deprecation")
  public Response createEmptyTable(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      Map<String, Object> requestBody,
      @Context HttpHeaders headers) {
    try {
      validateCreateEmptyTableRequest(requestBody);
      String tableLocation =
          Optional.ofNullable(requestBody)
              .map(body -> body.get(LANCE_LOCATION))
              .map(String::valueOf)
              .orElse(null);
      Map<String, String> props = extractPropertiesFromBody(requestBody);
      MultivaluedMap<String, String> headersMap = headers.getRequestHeaders();
      String tableProperties = headersMap.getFirst(LANCE_TABLE_PROPERTIES_PREFIX_HEADER);
      Map<String, String> headerProps = SerializationUtils.deserializeProperties(tableProperties);
      // Keep backward compatibility: accept body properties and let header override on key
      // conflict.
      props.putAll(headerProps);

      CreateEmptyTableResponse response =
          lanceNamespace.asTableOps().createEmptyTable(tableId, delimiter, tableLocation, props);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/declare")
  @Produces("application/json")
  @Timed(name = "declare-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "declare-table", absolute = true)
  public Response declareTable(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      DeclareTableRequest declareTableRequest,
      @Context HttpHeaders headers) {
    try {
      validateDeclareTableRequest(declareTableRequest);
      String tableLocation =
          declareTableRequest.getLocation() != null ? declareTableRequest.getLocation() : null;
      MultivaluedMap<String, String> headersMap = headers.getRequestHeaders();
      String tableProperties = headersMap.getFirst(LANCE_TABLE_PROPERTIES_PREFIX_HEADER);
      Map<String, String> props = SerializationUtils.deserializeProperties(tableProperties);

      DeclareTableResponse response =
          lanceNamespace.asTableOps().declareTable(tableId, delimiter, tableLocation, props);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/register")
  @Timed(name = "register-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "register-table", absolute = true)
  public Response registerTable(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @Context HttpHeaders headers,
      RegisterTableRequest registerTableRequest) {
    try {
      validateRegisterTableRequest(registerTableRequest);

      Map<String, String> props =
          registerTableRequest.getProperties() == null
              ? Maps.newHashMap()
              : Maps.newHashMap(registerTableRequest.getProperties());
      props.put(LANCE_LOCATION, registerTableRequest.getLocation());
      props.put(LanceConstants.LANCE_TABLE_REGISTER, "true");
      String mode =
          registerTableRequest.getMode() == null ? "create" : registerTableRequest.getMode();

      RegisterTableResponse response =
          lanceNamespace.asTableOps().registerTable(tableId, mode, delimiter, props);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/deregister")
  @Timed(name = "deregister-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "deregister-table", absolute = true)
  public Response deregisterTable(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @Context HttpHeaders headers,
      DeregisterTableRequest deregisterTableRequest) {
    try {
      validateDeregisterTableRequest(deregisterTableRequest);
      DeregisterTableResponse response =
          lanceNamespace.asTableOps().deregisterTable(tableId, delimiter);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/exists")
  @Timed(name = "table-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table-exists", absolute = true)
  public Response tableExists(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @Context HttpHeaders headers,
      TableExistsRequest tableExistsRequest) {
    try {
      validateTableExists(tableExistsRequest);
      // True if exists, false if not found and, otherwise throws exception
      boolean exists = lanceNamespace.asTableOps().tableExists(tableId, delimiter);
      if (exists) {
        return Response.status(Response.Status.OK).build();
      }
      throw new TableNotFoundException("Table not found: " + tableId, null, tableId);
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/drop")
  @Timed(name = "drop-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-table", absolute = true)
  public Response dropTable(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @Context HttpHeaders headers,
      DropTableRequest dropTableRequest) {
    try {
      validateDropTableRequest(dropTableRequest);
      DropTableResponse response = lanceNamespace.asTableOps().dropTable(tableId, delimiter);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/drop_columns")
  @Timed(name = "drop-columns." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-columns", absolute = true)
  public Response dropColumns(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @Context HttpHeaders headers,
      AlterTableDropColumnsRequest alterTableDropColumnsRequest) {
    try {
      validateDropColumnsRequest(alterTableDropColumnsRequest);
      AlterTableDropColumnsResponse response =
          (AlterTableDropColumnsResponse)
              lanceNamespace
                  .asTableOps()
                  .alterTable(tableId, delimiter, alterTableDropColumnsRequest);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  // TODO: Currently, only column rename is supported in alter columns.
  @POST
  @Path("/alter_columns")
  @Timed(name = "alter-columns." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-columns", absolute = true)
  public Response alterColumns(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @Context HttpHeaders headers,
      AlterTableAlterColumnsRequest alterTableAlterColumnsRequest) {
    try {
      validateAlterColumnsRequest(alterTableAlterColumnsRequest);
      AlterTableAlterColumnsResponse response =
          (AlterTableAlterColumnsResponse)
              lanceNamespace
                  .asTableOps()
                  .alterTable(tableId, delimiter, alterTableAlterColumnsRequest);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @SuppressWarnings({"unused", "deprecation"})
  private void validateCreateEmptyTableRequest(Map<String, Object> requestBody) {
    // No specific fields to validate for now
  }

  private void validateDeclareTableRequest(
      @SuppressWarnings("unused") DeclareTableRequest request) {
    // No specific fields to validate for now
  }

  private static Map<String, String> extractPropertiesFromBody(Map<String, Object> requestBody) {
    if (requestBody == null) {
      return Maps.newHashMap();
    }

    Object propertiesObject = requestBody.get("properties");
    if (!(propertiesObject instanceof Map<?, ?>)) {
      return Maps.newHashMap();
    }

    Map<String, String> properties = Maps.newHashMap();
    ((Map<?, ?>) propertiesObject)
        .forEach((key, value) -> properties.put(String.valueOf(key), String.valueOf(value)));
    return properties;
  }

  private void validateRegisterTableRequest(
      @SuppressWarnings("unused") RegisterTableRequest request) {
    // No specific fields to validate for now
  }

  private void validateDeregisterTableRequest(
      @SuppressWarnings("unused") DeregisterTableRequest request) {
    // We will ignore the id in the request body since it's already provided in the path param.
    // No specific fields to validate for now
  }

  private void validateDescribeTableRequest(
      @SuppressWarnings("unused") DescribeTableRequest request) {
    // We will ignore the id in the request body since it's already provided in the path param
    // No specific fields to validate for now
  }

  private void validateTableExists(@SuppressWarnings("unused") TableExistsRequest request) {
    // We will ignore the id in the request body since it's already provided in the path param
    // No specific fields to validate for now
  }

  private void validateDropTableRequest(@SuppressWarnings("unused") DropTableRequest request) {
    // We will ignore the id in the request body since it's already provided in the path param
    // No specific fields to validate for now
  }

  private void validateDropColumnsRequest(AlterTableDropColumnsRequest request) {
    Preconditions.checkArgument(
        !request.getColumns().isEmpty(), "Columns to drop cannot be empty.");
    Preconditions.checkArgument(
        request.getColumns().stream().allMatch(StringUtils::isNotBlank),
        "Columns to drop cannot be blank.");
  }

  private void validateAlterColumnsRequest(AlterTableAlterColumnsRequest request) {
    Preconditions.checkArgument(
        !request.getAlterations().isEmpty(), "Columns to alter cannot be empty.");

    for (AlterColumnsEntry alteration : request.getAlterations()) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(alteration.getPath()), "Column path to alter cannot be empty.");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(alteration.getRename()), "Rename field must be specified.");
      Preconditions.checkArgument(
          alteration.getDataType() == null
              && alteration.getNullable() == null
              && alteration.getVirtualColumn() == null,
          "Only RENAME alteration is supported currently.");
    }
  }

  private static String normalizeCreateTableMode(String mode) {
    if (mode == null) {
      return "create";
    }
    return mode;
  }
}
