/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest;

import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergRestUtils;
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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/{prefix:([^/]*/)?}namespaces")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergNamespaceOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergNamespaceOperations.class);

  private IcebergTableOps icebergTableOps;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public IcebergNamespaceOperations(IcebergTableOps icebergTableOps) {
    this.icebergTableOps = icebergTableOps;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response listNamespaces(@DefaultValue("") @QueryParam("parent") String parent) {
    Namespace parentNamespace =
        parent.isEmpty() ? Namespace.empty() : RESTUtil.decodeNamespace(parent);
    ListNamespacesResponse response = icebergTableOps.listNamespace(parentNamespace);
    return IcebergRestUtils.ok(response);
  }

  @GET
  @Path("{namespace}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response loadNamespace(@PathParam("namespace") String namespace) {
    GetNamespaceResponse getNamespaceResponse =
        icebergTableOps.loadNamespace(RESTUtil.decodeNamespace(namespace));
    return IcebergRestUtils.ok(getNamespaceResponse);
  }

  @DELETE
  @Path("{namespace}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response dropNamespace(@PathParam("namespace") String namespace) {
    // todo check if table exists in namespace after table ops is added
    LOG.info("Drop Iceberg namespace: {}", namespace);
    icebergTableOps.dropNamespace(RESTUtil.decodeNamespace(namespace));
    return IcebergRestUtils.noContent();
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public Response createNamespace(CreateNamespaceRequest namespaceRequest) {
    LOG.info("Create Iceberg namespace: {}", namespaceRequest);
    CreateNamespaceResponse response = icebergTableOps.createNamespace(namespaceRequest);
    return IcebergRestUtils.ok(response);
  }

  @POST
  @Path("{namespace}/properties")
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateNamespace(
      @PathParam("namespace") String namespace, UpdateNamespacePropertiesRequest request) {
    LOG.info("Update Iceberg namespace: {}, request: {}", namespace, request);
    UpdateNamespacePropertiesResponse response =
        icebergTableOps.updateNamespaceProperties(RESTUtil.decodeNamespace(namespace), request);
    return IcebergRestUtils.ok(response);
  }
}
