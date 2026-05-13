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
import com.google.common.base.Preconditions;
import java.util.List;
import javax.inject.Inject;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.lance.common.model.BatchCreateTableVersionsRequest;
import org.apache.gravitino.lance.common.model.BatchCreateTableVersionsResponse;
import org.apache.gravitino.lance.common.model.BatchDeleteTableVersionsRequest;
import org.apache.gravitino.lance.common.model.BatchDeleteTableVersionsResponse;
import org.apache.gravitino.lance.common.model.CreateTableVersionEntry;
import org.apache.gravitino.lance.common.model.CreateTableVersionRequest;
import org.apache.gravitino.lance.common.model.DescribeTableVersionRequest;
import org.apache.gravitino.lance.common.model.ListTableVersionsResponse;
import org.apache.gravitino.lance.common.model.TableVersionInfo;
import org.apache.gravitino.lance.common.ops.NamespaceWrapper;
import org.apache.gravitino.lance.service.LanceExceptionMapper;
import org.apache.gravitino.metrics.MetricNames;

@Path("/v1/table")
@Produces(MediaType.APPLICATION_JSON)
public class LanceTableVersionOperations {

  private final NamespaceWrapper lanceNamespace;

  @Inject
  public LanceTableVersionOperations(NamespaceWrapper lanceNamespace) {
    this.lanceNamespace = lanceNamespace;
  }

  @POST
  @Path("/{id}/version/create")
  @Timed(name = "create-table-version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-table-version", absolute = true)
  public Response createTableVersion(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      CreateTableVersionRequest request) {
    try {
      validateCreateTableVersionRequest(request);
      TableVersionInfo response =
          lanceNamespace.asTableOps().createTableVersion(tableId, delimiter, request);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @GET
  @Path("/{id}/version/list")
  @Timed(name = "list-table-versions." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-table-versions", absolute = true)
  public Response listTableVersions(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      @QueryParam("limit") Integer limit,
      @QueryParam("page_token") String pageToken,
      @QueryParam("descending") @DefaultValue("true") boolean descending) {
    try {
      ListTableVersionsResponse response =
          lanceNamespace
              .asTableOps()
              .listTableVersions(tableId, delimiter, limit, descending, pageToken);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/{id}/version/describe")
  @Timed(name = "describe-table-version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "describe-table-version", absolute = true)
  public Response describeTableVersion(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      DescribeTableVersionRequest request) {
    try {
      validateDescribeTableVersionRequest(request);
      TableVersionInfo response =
          lanceNamespace.asTableOps().describeTableVersion(tableId, delimiter, request);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  @POST
  @Path("/version/batch-create")
  @Timed(name = "batch-create-table-versions." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "batch-create-table-versions", absolute = true)
  public Response batchCreateTableVersions(BatchCreateTableVersionsRequest request) {
    try {
      validateBatchCreateTableVersionsRequest(request);
      BatchCreateTableVersionsResponse response =
          lanceNamespace.asTableOps().batchCreateTableVersions(request);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse("batch-create", e);
    }
  }

  @POST
  @Path("/{id}/version/batch-delete")
  @Timed(name = "batch-delete-table-versions." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "batch-delete-table-versions", absolute = true)
  public Response batchDeleteTableVersions(
      @PathParam("id") String tableId,
      @QueryParam("delimiter") @DefaultValue(NAMESPACE_DELIMITER_DEFAULT) String delimiter,
      BatchDeleteTableVersionsRequest request) {
    try {
      validateBatchDeleteTableVersionsRequest(request);
      BatchDeleteTableVersionsResponse response =
          lanceNamespace.asTableOps().batchDeleteTableVersions(tableId, delimiter, request);
      return Response.ok(response).build();
    } catch (Exception e) {
      return LanceExceptionMapper.toRESTResponse(tableId, e);
    }
  }

  private void validateCreateTableVersionRequest(CreateTableVersionRequest request) {
    Preconditions.checkArgument(request != null, "Request body is required.");
    Preconditions.checkArgument(request.getVersion() != null, "version is required.");
    Preconditions.checkArgument(request.getVersion() > 0, "version must be positive.");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(request.getManifestPath()), "manifest_path is required.");
  }

  private void validateDescribeTableVersionRequest(DescribeTableVersionRequest request) {
    Preconditions.checkArgument(request != null, "Request body is required.");
    Preconditions.checkArgument(request.getVersion() != null, "version is required.");
    Preconditions.checkArgument(request.getVersion() > 0, "version must be positive.");
  }

  private void validateBatchCreateTableVersionsRequest(BatchCreateTableVersionsRequest request) {
    Preconditions.checkArgument(request != null, "Request body is required.");
    Preconditions.checkArgument(
        request.getEntries() != null && !request.getEntries().isEmpty(), "entries are required.");
    for (CreateTableVersionEntry entry : request.getEntries()) {
      Preconditions.checkArgument(entry.getId() != null && entry.getId().size() == 3, "id must contain 3 levels.");
      validateCreateTableVersionRequest(entry);
    }
  }

  private void validateBatchDeleteTableVersionsRequest(BatchDeleteTableVersionsRequest request) {
    Preconditions.checkArgument(request != null, "Request body is required.");
    Preconditions.checkArgument(
        request.getVersions() != null && !request.getVersions().isEmpty(),
        "versions are required.");
    for (Long version : request.getVersions()) {
      Preconditions.checkArgument(version != null && version > 0, "version must be positive.");
    }
  }
}
