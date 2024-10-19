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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;

@Path("/v1/{prefix:([^/]*/)?}namespaces/{namespace}/views")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergViewOperations {

  private IcebergCatalogWrapperManager icebergCatalogWrapperManager;

  @SuppressWarnings("UnusedVariable")
  @Context
  private HttpServletRequest httpRequest;

  @Inject
  public IcebergViewOperations(IcebergCatalogWrapperManager icebergCatalogWrapperManager) {
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "list-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-view", absolute = true)
  public Response listView(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    ListTablesResponse response =
        icebergCatalogWrapperManager.getOps(prefix).listView(RESTUtil.decodeNamespace(namespace));
    return IcebergRestUtils.ok(response);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "create-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-view", absolute = true)
  public Response createView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      CreateViewRequest request) {
    LoadViewResponse response =
        icebergCatalogWrapperManager
            .getOps(prefix)
            .createView(RESTUtil.decodeNamespace(namespace), request);
    return IcebergRestUtils.ok(response);
  }

  @GET
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "load-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-view", absolute = true)
  public Response loadView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    TableIdentifier viewIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    LoadViewResponse response =
        icebergCatalogWrapperManager.getOps(prefix).loadView(viewIdentifier);
    return IcebergRestUtils.ok(response);
  }

  @POST
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "replace-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "replace-view", absolute = true)
  public Response replaceView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      UpdateTableRequest request) {
    TableIdentifier viewIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    LoadViewResponse response =
        icebergCatalogWrapperManager.getOps(prefix).updateView(viewIdentifier, request);
    return IcebergRestUtils.ok(response);
  }

  @DELETE
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "drop-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-view", absolute = true)
  public Response dropView(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    TableIdentifier viewIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    icebergCatalogWrapperManager.getOps(prefix).dropView(viewIdentifier);
    return IcebergRestUtils.noContent();
  }

  @HEAD
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "view-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "view-exits", absolute = true)
  public Response viewExists(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    TableIdentifier tableIdentifier = TableIdentifier.of(RESTUtil.decodeNamespace(namespace), view);
    if (icebergCatalogWrapperManager.getOps(prefix).existView(tableIdentifier)) {
      return IcebergRestUtils.noContent();
    } else {
      return IcebergRestUtils.notExists();
    }
  }
}
