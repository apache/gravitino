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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DeregisterTableRequest;
import com.lancedb.lance.namespace.model.DeregisterTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.RegisterTableRequest;
import com.lancedb.lance.namespace.model.RegisterTableResponse;
import com.lancedb.lance.namespace.util.JsonUtil;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
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
      @DefaultValue("$") @QueryParam("delimiter") String delimiter) {
    try {
      DescribeTableResponse response =
          lanceNamespace.asTableOps().describeTable(tableId, delimiter);
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
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @HeaderParam("x-lance-table-location") String tableLocation,
      @HeaderParam("x-lance-table-properties") String tableProperties,
      @HeaderParam("x-lance-root-catalog") String rootCatalog,
      byte[] arrowStreamBody) {
    try {
      Map<String, String> props =
          JsonUtil.mapper().readValue(tableProperties, new TypeReference<Map<String, String>>() {});
      CreateTableResponse response =
          lanceNamespace
              .asTableOps()
              .createTable(
                  tableId, mode, delimiter, tableLocation, props, rootCatalog, arrowStreamBody);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/create-empty")
  @Timed(name = "create-empty-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-empty-table", absolute = true)
  public Response createEmptyTable(
      @PathParam("id") String tableId,
      @QueryParam("mode") @DefaultValue("create") String mode, // create, exist_ok, overwrite
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @HeaderParam("x-lance-table-location") String tableLocation,
      @HeaderParam("x-lance-root-catalog") String rootCatalog,
      @HeaderParam("x-lance-table-properties") String tableProperties) {
    try {
      Map<String, String> props =
          StringUtils.isBlank(tableProperties)
              ? Map.of()
              : JsonUtil.mapper()
                  .readValue(tableProperties, new TypeReference<Map<String, String>>() {});
      CreateTableResponse response =
          lanceNamespace
              .asTableOps()
              .createTable(tableId, mode, delimiter, tableLocation, props, rootCatalog, null);
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
      @QueryParam("mode") @DefaultValue("create") String mode, // overwrite or
      @QueryParam("delimiter") @DefaultValue("$") String delimiter,
      @Context HttpHeaders headers,
      RegisterTableRequest registerTableRequest) {
    try {
      Map<String, String> props =
          registerTableRequest.getProperties() == null
              ? Map.of()
              : Maps.newHashMap(registerTableRequest.getProperties());
      // For lance spark compatibility
      String rootCatalog =
          headers.getRequestHeaders().get("x-lance-root-catalog") == null
              ? null
              : headers.getRequestHeaders().get("x-lance-root-catalog").get(0);
      props.put("register", "true");
      props.put("location", registerTableRequest.getLocation());
      props.put("format", "lance");

      RegisterTableResponse response =
          lanceNamespace.asTableOps().registerTable(tableId, mode, delimiter, props, rootCatalog);
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
      // For lance spark compatibility
      String rootCatalog =
          headers.getRequestHeaders().get("x-lance-root-catalog") == null
              ? null
              : headers.getRequestHeaders().get("x-lance-root-catalog").get(0);

      DeregisterTableResponse response =
          lanceNamespace.asTableOps().deregisterTable(tableId, delimiter, rootCatalog);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }
}
