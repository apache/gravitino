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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.metrics.MetricNames;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropNamespaceResponse;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesResponse;

@Path("/v1/namespace")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LanceNamespaceOperations {

  private static final String ROOT_NAMESPACE_ID = "";

  private final NamespaceWrapper lanceNamespace;

  @Inject
  public LanceNamespaceOperations(NamespaceWrapper lanceNamespace) {
    this.lanceNamespace = lanceNamespace;
  }

  @GET
  @Path("/{id}/list")
  @Timed(name = "list-namespaces." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-namespaces", absolute = true)
  public Response listNamespaces(
      @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      @QueryParam("page_token") String pageToken,
      @QueryParam("limit") Integer limit) {
    return listNamespacesInternal(namespaceId, delimiter, pageToken, limit);
  }

  @GET
  @Path("/list")
  @Timed(name = "list-namespaces-root." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-namespaces-root", absolute = true)
  public Response listNamespacesOnRoot(
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      @QueryParam("page_token") String pageToken,
      @QueryParam("limit") Integer limit) {
    return listNamespacesInternal(ROOT_NAMESPACE_ID, delimiter, pageToken, limit);
  }

  private Response listNamespacesInternal(
      String namespaceId, String delimiter, String pageToken, Integer limit) {
    try {
      ListNamespacesResponse response =
          lanceNamespace
              .asNamespaceOps()
              .listNamespaces(namespaceId, Pattern.quote(delimiter), pageToken, limit);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }
  }

  @POST
  @Path("/{id}/describe")
  @Timed(name = "describe-namespaces." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "describe-namespaces", absolute = true)
  public Response describeNamespace(
      @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter) {
    return describeNamespaceInternal(namespaceId, delimiter);
  }

  @POST
  @Path("/describe")
  @Timed(name = "describe-namespaces-root." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "describe-namespaces-root", absolute = true)
  public Response describeNamespaceOnRoot(
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter) {
    return describeNamespaceInternal(ROOT_NAMESPACE_ID, delimiter);
  }

  private Response describeNamespaceInternal(String namespaceId, String delimiter) {
    try {
      DescribeNamespaceResponse response =
          lanceNamespace.asNamespaceOps().describeNamespace(namespaceId, Pattern.quote(delimiter));
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }
  }

  @POST
  @Path("/{id}/create")
  @Timed(name = "create-namespaces." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-namespaces", absolute = true)
  public Response createNamespace(
      @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      CreateNamespaceRequest request) {
    try {
      CreateNamespaceResponse response =
          lanceNamespace
              .asNamespaceOps()
              .createNamespace(
                  namespaceId,
                  Pattern.quote(delimiter),
                  request.getMode() == null ? "create" : request.getMode(),
                  request.getProperties());
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }
  }

  @POST
  @Path("/{id}/drop")
  @Timed(name = "drop-namespaces." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-namespaces", absolute = true)
  public Response dropNamespace(
      @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      DropNamespaceRequest request) {
    try {
      DropNamespaceResponse response =
          lanceNamespace
              .asNamespaceOps()
              .dropNamespace(
                  namespaceId,
                  Pattern.quote(delimiter),
                  request.getMode() == null ? "fail" : request.getMode(),
                  request.getBehavior() == null ? "restrict" : request.getBehavior());
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }
  }

  @POST
  @Path("/{id}/exists")
  @Timed(name = "namespace-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "namespace-exists", absolute = true)
  public Response namespaceExists(
      @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter) {
    try {
      lanceNamespace.asNamespaceOps().namespaceExists(namespaceId, Pattern.quote(delimiter));
      return Response.ok().build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }
  }

  @GET
  @Path("{id}/table/list")
  @Timed(name = "list-tables." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-tables", absolute = true)
  public Response listTables(
      @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      @QueryParam("page_token") String pageToken,
      @QueryParam("limit") Integer limit) {
    try {
      ListTablesResponse response =
          lanceNamespace.asNamespaceOps().listTables(namespaceId, delimiter, pageToken, limit);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(namespaceId, e);
    }
  }
}
