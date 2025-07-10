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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergNamespaceOperationDispatcher;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
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

  private ObjectMapper icebergObjectMapper;
  private IcebergNamespaceOperationDispatcher namespaceOperationDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public IcebergNamespaceOperations(
      IcebergNamespaceOperationDispatcher namespaceOperationDispatcher) {
    this.namespaceOperationDispatcher = namespaceOperationDispatcher;
    this.icebergObjectMapper = IcebergObjectMapper.getInstance();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "list-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-namespace", absolute = true)
  public Response listNamespaces(
      @DefaultValue("") @Encoded() @QueryParam("parent") String parent,
      @PathParam("prefix") String prefix) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace parentNamespace =
        parent.isEmpty() ? Namespace.empty() : RESTUtil.decodeNamespace(parent);
    LOG.info(
        "List Iceberg namespaces, catalog: {}, parentNamespace: {}", catalogName, parentNamespace);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            ListNamespacesResponse response =
                namespaceOperationDispatcher.listNamespaces(context, parentNamespace);
            return IcebergRestUtils.ok(response);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @GET
  @Path("{namespace}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "load-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-namespace", absolute = true)
  public Response loadNamespace(
      @PathParam("prefix") String prefix, @Encoded() @PathParam("namespace") String namespace) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info("Load Iceberg namespace, catalog: {}, namespace: {}", catalogName, icebergNS);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            GetNamespaceResponse getNamespaceResponse =
                namespaceOperationDispatcher.loadNamespace(context, icebergNS);
            return IcebergRestUtils.ok(getNamespaceResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @HEAD
  @Path("{namespace}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "namespace-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "namespace-exists", absolute = true)
  public Response namespaceExists(
      @PathParam("prefix") String prefix, @Encoded() @PathParam("namespace") String namespace) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info("Check Iceberg namespace exists, catalog: {}, namespace: {}", catalogName, icebergNS);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            boolean exists = namespaceOperationDispatcher.namespaceExists(context, icebergNS);
            if (exists) {
              return IcebergRestUtils.noContent();
            } else {
              return IcebergRestUtils.notExists();
            }
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @DELETE
  @Path("{namespace}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "drop-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-namespace", absolute = true)
  public Response dropNamespace(
      @PathParam("prefix") String prefix, @Encoded() @PathParam("namespace") String namespace) {
    // todo check if table exists in namespace after table ops is added
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info("Drop Iceberg namespace, catalog: {}, namespace: {}", catalogName, icebergNS);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            namespaceOperationDispatcher.dropNamespace(context, icebergNS);
            return IcebergRestUtils.noContent();
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "create-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-namespace", absolute = true)
  public Response createNamespace(
      @PathParam("prefix") String prefix, CreateNamespaceRequest createNamespaceRequest) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    LOG.info(
        "Create Iceberg namespace, catalog: {}, createNamespaceRequest: {}",
        catalogName,
        createNamespaceRequest);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            CreateNamespaceResponse createNamespaceResponse =
                namespaceOperationDispatcher.createNamespace(context, createNamespaceRequest);
            return IcebergRestUtils.ok(createNamespaceResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @POST
  @Path("{namespace}/properties")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "update-namespace." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-namespace", absolute = true)
  public Response updateNamespace(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Update Iceberg namespace, catalog: {}, namespace: {}, updateNamespacePropertiesRequest: {}",
        catalogName,
        icebergNS,
        SerializeUpdateNamespacePropertiesRequest(updateNamespacePropertiesRequest));
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            UpdateNamespacePropertiesResponse updateNamespacePropertiesResponse =
                namespaceOperationDispatcher.updateNamespace(
                    context, icebergNS, updateNamespacePropertiesRequest);
            return IcebergRestUtils.ok(updateNamespacePropertiesResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @POST
  @Path("{namespace}/register")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "register-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "register-table", absolute = true)
  public Response registerTable(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      RegisterTableRequest registerTableRequest) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Register Iceberg table, catalog: {}, namespace: {}, registerTableRequest: {}",
        catalogName,
        icebergNS,
        registerTableRequest);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            LoadTableResponse loadTableResponse =
                namespaceOperationDispatcher.registerTable(
                    context, icebergNS, registerTableRequest);
            return IcebergRestUtils.ok(loadTableResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  // HTTP request is null in Jersey test, override with a mock request when testing.
  @VisibleForTesting
  HttpServletRequest httpServletRequest() {
    return httpRequest;
  }

  private String SerializeUpdateNamespacePropertiesRequest(
      UpdateNamespacePropertiesRequest updateNamespacePropertiesRequest) {
    try {
      return icebergObjectMapper.writeValueAsString(updateNamespacePropertiesRequest);
    } catch (JsonProcessingException e) {
      LOG.warn("Serialize update namespace properties failed", e);
      return updateNamespacePropertiesRequest.toString();
    }
  }
}
