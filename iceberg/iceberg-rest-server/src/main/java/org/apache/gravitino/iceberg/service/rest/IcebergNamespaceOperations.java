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
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/{prefix:([^/]*/)?}namespaces")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergNamespaceOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergNamespaceOperations.class);

  private IcebergCatalogWrapperManager icebergCatalogWrapperManager;

  @SuppressWarnings("UnusedVariable")
  @Context
  private HttpServletRequest httpRequest;

  @Inject
  public IcebergNamespaceOperations(IcebergCatalogWrapperManager icebergCatalogWrapperManager) {
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "list-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-namespace", absolute = true)
  public Response listNamespaces(
      @DefaultValue("") @QueryParam("parent") String parent, @PathParam("prefix") String prefix) {
    Namespace parentNamespace =
        parent.isEmpty() ? Namespace.empty() : RESTUtil.decodeNamespace(parent);
    ListNamespacesResponse response =
        icebergCatalogWrapperManager.getOps(prefix).listNamespace(parentNamespace);
    return IcebergRestUtils.ok(response);
  }

  @GET
  @Path("{namespace}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "load-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-namespace", absolute = true)
  public Response loadNamespace(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    GetNamespaceResponse getNamespaceResponse =
        icebergCatalogWrapperManager
            .getOps(prefix)
            .loadNamespace(RESTUtil.decodeNamespace(namespace));
    return IcebergRestUtils.ok(getNamespaceResponse);
  }

  @DELETE
  @Path("{namespace}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "drop-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-namespace", absolute = true)
  public Response dropNamespace(
      @PathParam("prefix") String prefix, @PathParam("namespace") String namespace) {
    // todo check if table exists in namespace after table ops is added
    LOG.info("Drop Iceberg namespace: {}, prefix: {}", namespace, prefix);
    icebergCatalogWrapperManager.getOps(prefix).dropNamespace(RESTUtil.decodeNamespace(namespace));
    return IcebergRestUtils.noContent();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "create-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-namespace", absolute = true)
  public Response createNamespace(
      @PathParam("prefix") String prefix, CreateNamespaceRequest namespaceRequest) {
    LOG.info("Create Iceberg namespace: {}, prefix: {}", namespaceRequest, prefix);
    CreateNamespaceResponse response =
        icebergCatalogWrapperManager.getOps(prefix).createNamespace(namespaceRequest);
    return IcebergRestUtils.ok(response);
  }

  @POST
  @Path("{namespace}/properties")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "update-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-namespace", absolute = true)
  public Response updateNamespace(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      UpdateNamespacePropertiesRequest request) {
    LOG.info("Update Iceberg namespace: {}, request: {}, prefix: {}", namespace, request, prefix);
    UpdateNamespacePropertiesResponse response =
        icebergCatalogWrapperManager
            .getOps(prefix)
            .updateNamespaceProperties(RESTUtil.decodeNamespace(namespace), request);
    return IcebergRestUtils.ok(response);
  }

  @POST
  @Path("{namespace}/register")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "register-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "register-table", absolute = true)
  public Response registerTable(
      @PathParam("prefix") String prefix,
      @PathParam("namespace") String namespace,
      RegisterTableRequest request) {
    LOG.info("Register table, namespace: {}, request: {}", namespace, request);
    LoadTableResponse response =
        icebergCatalogWrapperManager
            .getOps(prefix)
            .registerTable(RESTUtil.decodeNamespace(namespace), request);
    return IcebergRestUtils.ok(response);
  }
}
