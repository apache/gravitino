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
import com.google.common.collect.Maps;
import com.lancedb.lance.namespace.model.CreateEmptyTableRequest;
import com.lancedb.lance.namespace.model.CreateEmptyTableResponse;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableRequest;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropTableRequest;
import com.lancedb.lance.namespace.model.DropTableResponse;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableRequest.ModeEnum;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import com.lancedb.lance.namespace.model.TableExistsRequest;
import java.util.Map;
import java.util.Optional;
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
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.common.utils.SerializationUtils;
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.metrics.MetricNames;

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
      DescribeTableResponse response =
          lanceNamespace
              .asTableOps()
              .describeTable(tableId, delimiter, Optional.ofNullable(request.getVersion()));
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
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
      CreateTableRequest.ModeEnum modeEnum = CreateTableRequest.ModeEnum.fromValue(mode);
      Map<String, String> props = SerializationUtils.deserializeProperties(tableProperties);
      CreateTableResponse response =
          lanceNamespace
              .asTableOps()
              .createTable(tableId, modeEnum, delimiter, tableLocation, props, arrowStreamBody);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/create-empty")
  @Produces("application/json")
  @Timed(name = "create-empty-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-empty-table", absolute = true)
  public Response createEmptyTable(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      CreateEmptyTableRequest request,
      @Context HttpHeaders headers) {
    try {
      validateCreateEmptyTableRequest(request);

      String tableLocation = request.getLocation();
      Map<String, String> props =
          request.getProperties() == null
              ? Maps.newHashMap()
              : Maps.newHashMap(request.getProperties());

      CreateEmptyTableResponse response =
          lanceNamespace.asTableOps().createEmptyTable(tableId, delimiter, tableLocation, props);
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
      props.put("register", "true");
      ModeEnum mode = registerTableRequest.getMode();

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
      return Response.status(Response.Status.NOT_FOUND).build();
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

  private void validateCreateEmptyTableRequest(
      @SuppressWarnings("unused") CreateEmptyTableRequest request) {
    // No specific fields to validate for now
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
}
