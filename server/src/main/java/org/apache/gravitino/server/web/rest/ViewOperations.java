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
package org.apache.gravitino.server.web.rest;

import static org.apache.gravitino.dto.util.DTOConverters.fromDTOs;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.ViewDispatcher;
import org.apache.gravitino.dto.requests.ViewCreateRequest;
import org.apache.gravitino.dto.requests.ViewUpdateRequest;
import org.apache.gravitino.dto.requests.ViewUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.ViewResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.ViewChange;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/views")
public class ViewOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ViewOperations.class);

  private final ViewDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public ViewOperations(ViewDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-view", absolute = true)
  public Response listViews(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    LOG.info("Received list views request for schema: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace viewNS = NamespaceUtil.ofView(metalake, catalog, schema);
            NameIdentifier[] idents = dispatcher.listViews(viewNS);
            Response response = Utils.ok(new EntityListResponse(idents));
            LOG.info(
                "List {} views under schema: {}.{}.{}", idents.length, metalake, catalog, schema);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleViewException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-view", absolute = true)
  public Response createView(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      ViewCreateRequest request) {
    LOG.info(
        "Received create view request: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifierUtil.ofView(metalake, catalog, schema, request.getName());

            View view =
                dispatcher.createView(
                    ident,
                    request.getComment(),
                    fromDTOs(request.getColumns()),
                    DTOConverters.fromDTOs(request.getRepresentations()),
                    request.getDefaultCatalog(),
                    request.getDefaultSchema(),
                    request.getProperties());
            Response response = Utils.ok(new ViewResponse(DTOConverters.toDTO(view)));
            LOG.info("View created: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleViewException(
          OperationType.CREATE, request.getName(), schema, e);
    }
  }

  @GET
  @Path("{view}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-view", absolute = true)
  public Response loadView(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("view") String view) {
    LOG.info("Received load view request for view: {}.{}.{}.{}", metalake, catalog, schema, view);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofView(metalake, catalog, schema, view);
            View v = dispatcher.loadView(ident);
            Response response = Utils.ok(new ViewResponse(DTOConverters.toDTO(v)));
            LOG.info("View loaded: {}.{}.{}.{}", metalake, catalog, schema, view);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleViewException(OperationType.LOAD, view, schema, e);
    }
  }

  @PUT
  @Path("{view}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-view", absolute = true)
  public Response alterView(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("view") String view,
      ViewUpdatesRequest request) {
    LOG.info("Received alter view request: {}.{}.{}.{}", metalake, catalog, schema, view);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofView(metalake, catalog, schema, view);
            ViewChange[] changes =
                request.getUpdates().stream()
                    .map(ViewUpdateRequest::viewChange)
                    .toArray(ViewChange[]::new);
            View v = dispatcher.alterView(ident, changes);
            Response response = Utils.ok(new ViewResponse(DTOConverters.toDTO(v)));
            LOG.info("View altered: {}.{}.{}.{}", metalake, catalog, schema, v.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleViewException(OperationType.ALTER, view, schema, e);
    }
  }

  @DELETE
  @Path("{view}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-view." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-view", absolute = true)
  public Response dropView(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("view") String view) {
    LOG.info("Received drop view request: {}.{}.{}.{}", metalake, catalog, schema, view);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofView(metalake, catalog, schema, view);
            boolean dropped = dispatcher.dropView(ident);
            if (!dropped) {
              LOG.warn("Cannot find to be dropped view {} under schema {}", view, schema);
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("View dropped: {}.{}.{}.{}", metalake, catalog, schema, view);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleViewException(OperationType.DROP, view, schema, e);
    }
  }
}
