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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Arrays;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PATCH;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.MetalakeChange;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.dto.MetalakeDTO;
import org.apache.gravitino.dto.requests.MetalakeCreateRequest;
import org.apache.gravitino.dto.requests.MetalakeSetRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdateRequest;
import org.apache.gravitino.dto.requests.MetalakeUpdatesRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.MetalakeListResponse;
import org.apache.gravitino.dto.responses.MetalakeResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metalake.MetalakeDispatcher;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataFilterHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class MetalakeOperations {

  private static final Logger LOG = LoggerFactory.getLogger(MetalakeOperations.class);

  private final MetalakeDispatcher metalakeDispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public MetalakeOperations(MetalakeDispatcher dispatcher) {
    this.metalakeDispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-metalake", absolute = true)
  public Response listMetalakes() {
    LOG.info("Received list metalakes request.");
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Metalake[] metalakes = metalakeDispatcher.listMetalakes();
            metalakes =
                Arrays.stream(metalakes)
                    .filter(
                        metalake -> {
                          NameIdentifier[] nameIdentifiers =
                              new NameIdentifier[] {NameIdentifierUtil.ofMetalake(metalake.name())};
                          return MetadataFilterHelper.filterByExpression(
                                      metalake.name(),
                                      "METALAKE_USER",
                                      Entity.EntityType.METALAKE,
                                      nameIdentifiers)
                                  .length
                              > 0;
                        })
                    .toList()
                    .toArray(new Metalake[0]);
            MetalakeDTO[] metalakeDTOs =
                Arrays.stream(metalakes).map(DTOConverters::toDTO).toArray(MetalakeDTO[]::new);
            Response response = Utils.ok(new MetalakeListResponse(metalakeDTOs));
            LOG.info("List {} metalakes in Gravitino", metalakeDTOs.length);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(
          OperationType.LIST, Namespace.empty().toString(), e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-metalake", absolute = true)
  @AuthorizationExpression(
      expression = "SERVICE_ADMIN",
      errorMessage =
          "Only service admins can create metalakes, current user can't create the metalake,"
              + "  you should configure it in the server configuration first")
  public Response createMetalake(MetalakeCreateRequest request) {
    LOG.info("Received create metalake request for {}", request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofMetalake(request.getName());
            Metalake metalake =
                metalakeDispatcher.createMetalake(
                    ident, request.getComment(), request.getProperties());
            Response response = Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));
            LOG.info("Metalake created: {}", metalake.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.CREATE, request.getName(), e);
    }
  }

  @GET
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-metalake", absolute = true)
  @AuthorizationExpression(expression = "METALAKE_USER")
  public Response loadMetalake(
      @PathParam("name") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName) {
    LOG.info("Received load metalake request for metalake: {}", metalakeName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifierUtil.ofMetalake(metalakeName);
            Metalake metalake = metalakeDispatcher.loadMetalake(identifier);
            Response response = Utils.ok(new MetalakeResponse(DTOConverters.toDTO(metalake)));
            LOG.info("Metalake loaded: {}", metalake.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.LOAD, metalakeName, e);
    }
  }

  @PATCH
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "set-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "set-metalake", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER")
  public Response setMetalake(
      @PathParam("name") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName,
      MetalakeSetRequest request) {
    LOG.info("Received set request for metalake: {}", metalakeName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifierUtil.ofMetalake(metalakeName);
            if (request.isInUse()) {
              metalakeDispatcher.enableMetalake(identifier);
            } else {
              metalakeDispatcher.disableMetalake(identifier);
            }
            Response response = Utils.ok(new BaseResponse());
            LOG.info(
                "Successfully {} metalake: {}",
                request.isInUse() ? "enable" : "disable",
                metalakeName);
            return response;
          });

    } catch (Exception e) {
      LOG.info("Failed to {} metalake: {}", request.isInUse() ? "enable" : "disable", metalakeName);
      return ExceptionHandlers.handleMetalakeException(
          request.isInUse() ? OperationType.ENABLE : OperationType.DISABLE, metalakeName, e);
    }
  }

  @PUT
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-metalake", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER")
  public Response alterMetalake(
      @PathParam("name") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName,
      MetalakeUpdatesRequest updatesRequest) {
    LOG.info("Received alter metalake request for metalake: {}", metalakeName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            updatesRequest.validate();
            NameIdentifier identifier = NameIdentifierUtil.ofMetalake(metalakeName);
            MetalakeChange[] changes =
                updatesRequest.getUpdates().stream()
                    .map(MetalakeUpdateRequest::metalakeChange)
                    .toArray(MetalakeChange[]::new);
            Metalake updatedMetalake = metalakeDispatcher.alterMetalake(identifier, changes);
            Response response =
                Utils.ok(new MetalakeResponse(DTOConverters.toDTO(updatedMetalake)));
            LOG.info("Metalake altered: {}", updatedMetalake.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.ALTER, metalakeName, e);
    }
  }

  @DELETE
  @Path("{name}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-metalake." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-metalake", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER")
  public Response dropMetalake(
      @PathParam("name") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName,
      @DefaultValue("false") @QueryParam("force") boolean force) {
    LOG.info("Received drop metalake request for metalake: {}", metalakeName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier identifier = NameIdentifierUtil.ofMetalake(metalakeName);
            boolean dropped = metalakeDispatcher.dropMetalake(identifier, force);
            if (!dropped) {
              LOG.warn("Failed to drop metalake by name {}", metalakeName);
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Metalake dropped: {}", metalakeName);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleMetalakeException(OperationType.DROP, metalakeName, e);
    }
  }
}
