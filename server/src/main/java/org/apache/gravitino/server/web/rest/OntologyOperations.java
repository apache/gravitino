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
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.dto.ontology.OntologyDTO;
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OntologyListResponse;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.ontology.Ontology;
import org.apache.gravitino.ontology.OntologyChange;
import org.apache.gravitino.ontology.OntologyDefinition;
import org.apache.gravitino.ontology.OntologyDispatcher;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * REST operations for metalake-level ontologies.
 *
 * <p>An ontology is addressed by name within a metalake, so the resource is mounted under the
 * metalake and every operation names the metalake explicitly. Ontology authorization is
 * metalake-scoped: writes require metalake ownership, and reads require nothing beyond the metalake
 * access check that every metalake-level resource performs. Dedicated {@code MANAGE_ONTOLOGY} and
 * {@code USE_ONTOLOGY} privileges, and the ownership model they need, are planned as a follow-up.
 */
@Path("metalakes/{metalake}/ontologies")
public class OntologyOperations {

  private static final Logger LOG = LoggerFactory.getLogger(OntologyOperations.class);

  private static final String VND_GRAVITINO_V1_JSON = "application/vnd.gravitino.v1+json";

  private final OntologyDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  /**
   * Creates a new ontology REST resource.
   *
   * @param dispatcher The ontology dispatcher.
   */
  @Inject
  public OntologyOperations(OntologyDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * Lists the ontologies of a metalake.
   *
   * @param metalake The metalake name.
   * @param verbose Whether to return the ontologies with their definitions.
   * @return The ontology names, or the ontologies themselves when {@code verbose} is true.
   */
  @GET
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "list-ontologies." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-ontologies", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listOntologies(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    LOG.info(
        "Received list ontology {} request for metalake: {}",
        verbose ? "infos" : "names",
        metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (verbose) {
              Ontology[] ontologies = dispatcher.listOntologiesInfo(metalake);
              OntologyDTO[] dtos =
                  ontologies == null
                      ? new OntologyDTO[0]
                      : Arrays.stream(ontologies)
                          .map(OntologyDTO::fromOntology)
                          .toArray(OntologyDTO[]::new);
              return Utils.ok(new OntologyListResponse(dtos));
            }

            String[] names = dispatcher.listOntologies(metalake);
            return Utils.ok(new NameListResponse(names == null ? new String[0] : names));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleOntologyException(OperationType.LIST, "", metalake, e);
    }
  }

  /**
   * Creates an ontology in a metalake.
   *
   * @param metalake The metalake name.
   * @param request The create request.
   * @return The created ontology.
   */
  @POST
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "create-ontology." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-ontology", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER")
  public Response createOntology(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      OntologyCreateRequest request) {
    if (request == null) {
      LOG.warn("Received create ontology request with null request body");
      return ExceptionHandlers.handleOntologyException(
          OperationType.CREATE,
          "",
          metalake,
          new IllegalArgumentException("Request body cannot be null"));
    }

    String name = request.getName();
    LOG.info("Received create ontology request under metalake: {}", metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            OntologyDefinition definition = request.getDefinition().toOntologyDefinition();
            Ontology ontology =
                dispatcher.createOntology(
                    metalake, name, request.getComment(), request.getProperties(), definition);
            LOG.info("Created ontology: {} under metalake: {}", ontology.name(), metalake);
            return Utils.ok(new OntologyResponse(OntologyDTO.fromOntology(ontology)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleOntologyException(OperationType.CREATE, name, metalake, e);
    }
  }

  /**
   * Gets an ontology.
   *
   * @param metalake The metalake name.
   * @param ontology The ontology name.
   * @return The ontology.
   */
  @GET
  @Path("{ontology}")
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "get-ontology." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-ontology", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response getOntology(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology) {
    LOG.info("Received get ontology request: {} under metalake: {}", ontology, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () ->
              Utils.ok(
                  new OntologyResponse(
                      OntologyDTO.fromOntology(dispatcher.getOntology(metalake, ontology)))));
    } catch (Exception e) {
      return ExceptionHandlers.handleOntologyException(OperationType.LOAD, ontology, metalake, e);
    }
  }

  /**
   * Alters an ontology.
   *
   * @param metalake The metalake name.
   * @param ontology The ontology name.
   * @param request The updates to apply.
   * @return The altered ontology.
   */
  @PUT
  @Path("{ontology}")
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "alter-ontology." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-ontology", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER")
  public Response alterOntology(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology,
      OntologyUpdatesRequest request) {
    if (request == null) {
      LOG.warn("Received alter ontology request with null request body");
      return ExceptionHandlers.handleOntologyException(
          OperationType.ALTER,
          ontology,
          metalake,
          new IllegalArgumentException("Request body cannot be null"));
    }

    LOG.info("Received alter ontology request: {} under metalake: {}", ontology, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            OntologyChange[] changes =
                request.getUpdates().stream()
                    .map(OntologyUpdateRequest::ontologyChange)
                    .toArray(OntologyChange[]::new);
            Ontology altered = dispatcher.alterOntology(metalake, ontology, changes);
            LOG.info("Altered ontology: {} under metalake: {}", ontology, metalake);
            return Utils.ok(new OntologyResponse(OntologyDTO.fromOntology(altered)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleOntologyException(OperationType.ALTER, ontology, metalake, e);
    }
  }

  /**
   * Deletes an ontology.
   *
   * @param metalake The metalake name.
   * @param ontology The ontology name.
   * @return Whether the ontology was deleted.
   */
  @DELETE
  @Path("{ontology}")
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "delete-ontology." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "delete-ontology", absolute = true)
  @AuthorizationExpression(expression = "METALAKE::OWNER")
  public Response deleteOntology(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology) {
    LOG.info("Received delete ontology request: {} under metalake: {}", ontology, metalake);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean deleted = dispatcher.deleteOntology(metalake, ontology);
            return Utils.ok(new DropResponse(deleted));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleOntologyException(OperationType.DROP, ontology, metalake, e);
    }
  }
}
