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
import java.util.ArrayList;
import java.util.List;
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
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergViewOperationDispatcher;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
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
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response listView(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
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

            IcebergRESTServerContext authContext = IcebergRESTServerContext.getInstance();
            if (authContext.isAuthorizationEnabled()) {
              listTablesResponse =
                  filterListViewsResponse(
                      listTablesResponse, authContext.metalakeName(), catalogName);
            }
            return IcebergRESTUtils.ok(listTablesResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "create-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-view", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_CREATE_VIEW",
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response createView(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      CreateViewRequest createViewRequest) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
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
            return IcebergRESTUtils.ok(loadViewResponse);
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
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (VIEW::OWNER || ANY_SELECT_VIEW || ANY_CREATE_VIEW)",
      accessMetadataType = MetadataObject.Type.VIEW)
  public Response loadView(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = EntityType.VIEW) @Encoded() @PathParam("view") String view) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String viewName = RESTUtil.decodeString(view);
    LOG.info(
        "Load Iceberg view, catalog: {}, namespace: {}, view: {}",
        catalogName,
        icebergNS,
        viewName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, viewName);
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            LoadViewResponse loadViewResponse =
                viewOperationDispatcher.loadView(context, viewIdentifier);
            return IcebergRESTUtils.ok(loadViewResponse);
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
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && VIEW::OWNER",
      accessMetadataType = MetadataObject.Type.VIEW)
  public Response replaceView(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = EntityType.VIEW) @Encoded() @PathParam("view") String view,
      UpdateTableRequest replaceViewRequest) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String viewName = RESTUtil.decodeString(view);
    LOG.info(
        "Replace Iceberg view, catalog: {}, namespace: {}, view: {}, replaceViewRequest: {}",
        catalogName,
        icebergNS,
        viewName,
        SerializeReplaceViewRequest(replaceViewRequest));
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, viewName);
            LoadViewResponse loadViewResponse =
                viewOperationDispatcher.replaceView(context, viewIdentifier, replaceViewRequest);
            return IcebergRESTUtils.ok(loadViewResponse);
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
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && VIEW::OWNER",
      accessMetadataType = MetadataObject.Type.VIEW)
  public Response dropView(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = EntityType.VIEW) @Encoded() @PathParam("view") String view) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String viewName = RESTUtil.decodeString(view);
    LOG.info(
        "Drop Iceberg view, catalog: {}, namespace: {}, view: {}",
        catalogName,
        icebergNS,
        viewName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, viewName);
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            viewOperationDispatcher.dropView(context, viewIdentifier);
            return IcebergRESTUtils.noContent();
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @HEAD
  @Path("{view}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "view-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "view-exists", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (VIEW::OWNER || ANY_SELECT_VIEW || ANY_CREATE_VIEW)",
      accessMetadataType = MetadataObject.Type.VIEW)
  public Response viewExists(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = EntityType.VIEW) @Encoded() @PathParam("view") String view) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String viewName = RESTUtil.decodeString(view);
    LOG.info(
        "Check Iceberg view exists, catalog: {}, namespace: {}, view: {}",
        catalogName,
        icebergNS,
        viewName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            TableIdentifier viewIdentifier = TableIdentifier.of(icebergNS, viewName);
            boolean exists = viewOperationDispatcher.viewExists(context, viewIdentifier);
            if (exists) {
              return IcebergRESTUtils.noContent();
            } else {
              return IcebergRESTUtils.notExists();
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

  private NameIdentifier[] toViewNameIdentifiers(
      ListTablesResponse listTablesResponse, String metalake, String catalogName) {
    List<TableIdentifier> identifiers = listTablesResponse.identifiers();
    NameIdentifier[] nameIdentifiers = new NameIdentifier[identifiers.size()];
    for (int i = 0; i < identifiers.size(); i++) {
      TableIdentifier identifier = identifiers.get(i);
      nameIdentifiers[i] =
          NameIdentifier.of(
              metalake, catalogName, identifier.namespace().level(0), identifier.name());
    }
    return nameIdentifiers;
  }

  private ListTablesResponse filterListViewsResponse(
      ListTablesResponse listTablesResponse, String metalake, String catalogName) {
    NameIdentifier[] idents =
        MetadataAuthzHelper.filterByExpression(
            metalake,
            AuthorizationExpressionConstants.FILTER_VIEW_AUTHORIZATION_EXPRESSION,
            Entity.EntityType.VIEW,
            toViewNameIdentifiers(listTablesResponse, metalake, catalogName));
    List<TableIdentifier> filteredIdentifiers = new ArrayList<>();
    for (NameIdentifier ident : idents) {
      filteredIdentifiers.add(
          TableIdentifier.of(Namespace.of(ident.namespace().level(0)), ident.name()));
    }
    return ListTablesResponse.builder()
        .addAll(filteredIdentifiers)
        .nextPageToken(listTablesResponse.nextPageToken())
        .build();
  }
}
