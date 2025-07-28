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
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRestUtils;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationDispatcher;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.web.Utils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/{prefix:([^/]*/)?}namespaces/{namespace}/views")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergViewOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergViewOperations.class);

  private ObjectMapper icebergObjectMapper;
  private IcebergViewOperationDispatcher viewOperationDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public IcebergViewOperations(IcebergViewOperationDispatcher viewOperationDispatcher) {
    this.viewOperationDispatcher = viewOperationDispatcher;
    this.icebergObjectMapper = IcebergObjectMapper.getInstance();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "list-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-view", absolute = true)
  public Response listView(
      @PathParam("prefix") String prefix, @Encoded() @PathParam("namespace") String namespace) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info("List Iceberg views, catalog: {}, namespace: {}", catalogName, icebergNS);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            ListTablesResponse listTablesResponse =
                viewOperationDispatcher.listView(context, icebergNS);
            return IcebergRestUtils.ok(listTablesResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "create-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-view", absolute = true)
  public Response createView(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      CreateViewRequest createViewRequest) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Create Iceberg view, catalog: {}, namespace: {}, createViewRequest: {}",
        catalogName,
        icebergNS,
        createViewRequest);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            LoadViewResponse loadViewResponse =
                viewOperationDispatcher.createView(context, icebergNS, createViewRequest);
            return IcebergRestUtils.ok(loadViewResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @GET
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "load-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-view", absolute = true)
  public Response loadView(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Load Iceberg view, catalog: {}, namespace: {}, view: {}", catalogName, icebergNS, view);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, view);
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            LoadViewResponse loadViewResponse =
                viewOperationDispatcher.loadView(context, viewIdentifier);
            return IcebergRestUtils.ok(loadViewResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @POST
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "replace-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "replace-view", absolute = true)
  public Response replaceView(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      @PathParam("view") String view,
      UpdateTableRequest replaceViewRequest) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Replace Iceberg view, catalog: {}, namespace: {}, view: {}, replaceViewRequest: {}",
        catalogName,
        icebergNS,
        view,
        SerializeReplaceViewRequest(replaceViewRequest));
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, view);
            LoadViewResponse loadViewResponse =
                viewOperationDispatcher.replaceView(context, viewIdentifier, replaceViewRequest);
            return IcebergRestUtils.ok(loadViewResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @DELETE
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "drop-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-view", absolute = true)
  public Response dropView(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Drop Iceberg view, catalog: {}, namespace: {}, view: {}", catalogName, icebergNS, view);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, view);
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            viewOperationDispatcher.dropView(context, viewIdentifier);
            return IcebergRestUtils.noContent();
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @HEAD
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "view-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "view-exits", absolute = true)
  public Response viewExists(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      @PathParam("view") String view) {
    String catalogName = IcebergRestUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Check Iceberg view exists, catalog: {}, namespace: {}, view: {}",
        catalogName,
        icebergNS,
        view);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, view);
            boolean exists = viewOperationDispatcher.viewExists(context, viewIdentifier);
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

  // HTTP request is null in Jersey test, override with a mock request when testing.
  @VisibleForTesting
  HttpServletRequest httpServletRequest() {
    return httpRequest;
  }

  private String SerializeReplaceViewRequest(UpdateTableRequest replaceViewRequest) {
    try {
      return icebergObjectMapper.writeValueAsString(replaceViewRequest);
    } catch (JsonProcessingException e) {
      LOG.warn("Serialize update view request failed", e);
      return replaceViewRequest.toString();
    }
  }
}
