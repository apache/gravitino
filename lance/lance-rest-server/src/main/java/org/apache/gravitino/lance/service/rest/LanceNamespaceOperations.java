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
import com.lancedb.lance.namespace.model.CreateNamespaceRequest;
import com.lancedb.lance.namespace.model.CreateNamespaceResponse;
import com.lancedb.lance.namespace.model.DescribeNamespaceResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropNamespaceResponse;
import com.lancedb.lance.namespace.model.ListNamespacesResponse;
import com.lancedb.lance.namespace.model.ListTablesResponse;
import java.util.regex.Pattern;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
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

@Path("/v1/namespace")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class LanceNamespaceOperations {

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
      @Encoded @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      @QueryParam("page_token") String pageToken,
      @QueryParam("limit") Integer limit) {
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
      @Encoded @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter) {
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
      @Encoded @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      CreateNamespaceRequest request) {
    try {
      CreateNamespaceResponse response =
          lanceNamespace
              .asNamespaceOps()
              .createNamespace(
                  namespaceId,
                  Pattern.quote(delimiter),
                  request.getMode() == null
                      ? CreateNamespaceRequest.ModeEnum.CREATE
                      : request.getMode(),
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
      @Encoded @PathParam("id") String namespaceId,
      @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) @QueryParam("delimiter") String delimiter,
      DropNamespaceRequest request) {
    try {
      DropNamespaceResponse response =
          lanceNamespace
              .asNamespaceOps()
              .dropNamespace(
                  namespaceId,
                  Pattern.quote(delimiter),
                  request.getMode() == null
                      ? DropNamespaceRequest.ModeEnum.FAIL
                      : request.getMode(),
                  request.getBehavior() == null
                      ? DropNamespaceRequest.BehaviorEnum.RESTRICT
                      : request.getBehavior());
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
      @Encoded @PathParam("id") String namespaceId,
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
