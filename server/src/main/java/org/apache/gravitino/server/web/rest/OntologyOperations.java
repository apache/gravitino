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

import static org.apache.gravitino.server.web.rest.OntologyRestSupport.VND_GRAVITINO_V1_JSON;
import static org.apache.gravitino.server.web.rest.OntologyRestSupport.YAML;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.dto.ontology.OntologyDTO;
import org.apache.gravitino.dto.requests.OntologyCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.NameListResponse;
import org.apache.gravitino.dto.responses.OntologyResponse;
import org.apache.gravitino.ontology.OntologyDispatcher;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** REST endpoints for metalake-wide ontologies. */
@Path("/metalakes/{metalake}/ontologies")
@Consumes({VND_GRAVITINO_V1_JSON, MediaType.APPLICATION_JSON})
@Produces(VND_GRAVITINO_V1_JSON)
public class OntologyOperations {

  private final OntologyDispatcher dispatcher;
  @Context private HttpServletRequest request;

  /**
   * Constructs the resource.
   *
   * @param dispatcher The ontology dispatcher.
   */
  @Inject
  public OntologyOperations(OntologyDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * Lists visible ontologies.
   *
   * @param metalake The containing metalake.
   * @return The REST response.
   */
  @GET
  @AuthorizationExpression(expression = "")
  public Response list(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake) {
    return OntologyRestSupport.run(
        request,
        () -> {
          String[] names =
              dispatcher.listOntologies(metalake).stream()
                  .filter(
                      name ->
                          MetadataAuthzHelper.checkAccess(
                              NameIdentifierUtil.ofOntology(metalake, name),
                              Entity.EntityType.ONTOLOGY,
                              AuthorizationExpressionConstants
                                  .LOAD_ONTOLOGY_AUTHORIZATION_EXPRESSION))
                  .toArray(String[]::new);
          return Utils.ok(new NameListResponse(names));
        });
  }

  /**
   * Creates an ontology.
   *
   * @param metalake The containing metalake.
   * @param body The native create request.
   * @return The REST response.
   */
  @POST
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.CREATE_ONTOLOGY_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.METALAKE)
  public Response create(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      OntologyCreateRequest body) {
    return OntologyRestSupport.run(
        request,
        () -> {
          OntologyRestSupport.validate(body);
          return Utils.ok(
              new OntologyResponse(
                  OntologyDTO.from(
                      dispatcher.createOntology(
                          metalake,
                          body.name(),
                          body.comment(),
                          body.definition(),
                          body.properties()))));
        });
  }

  /**
   * Imports a standalone Ossie ontology.
   *
   * @param metalake The containing metalake.
   * @param document The raw Ossie input.
   * @param headers The input media type.
   * @return The REST response.
   */
  @POST
  @Path("ossie")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.CREATE_ONTOLOGY_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.METALAKE)
  @Consumes({MediaType.APPLICATION_JSON, YAML, "application/x-yaml", "text/yaml"})
  public Response importDocument(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      String document,
      @Context HttpHeaders headers) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return Utils.ok(
              new OntologyResponse(
                  OntologyDTO.from(
                      dispatcher.importOntology(
                          metalake, OntologyRestSupport.document(document, headers)))));
        });
  }

  /**
   * Loads an ontology.
   *
   * @param metalake The containing metalake.
   * @param ontology The ontology name.
   * @return The REST response.
   */
  @GET
  @Path("{ontology}")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_ONTOLOGY_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.ONTOLOGY)
  public Response load(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return Utils.ok(
              new OntologyResponse(OntologyDTO.from(dispatcher.loadOntology(metalake, ontology))));
        });
  }

  /**
   * Alters an ontology.
   *
   * @param metalake The containing metalake.
   * @param ontology The ontology name.
   * @param body The ordered changes.
   * @return The REST response.
   */
  @PUT
  @Path("{ontology}")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.MODIFY_ONTOLOGY_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.ONTOLOGY)
  public Response alter(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology,
      OntologyUpdatesRequest body) {
    return OntologyRestSupport.run(
        request,
        () -> {
          OntologyRestSupport.validate(body);
          return Utils.ok(
              new OntologyResponse(
                  OntologyDTO.from(
                      dispatcher.alterOntology(
                          metalake,
                          ontology,
                          OntologyRestSupport::canLoadMapping,
                          body.ontologyChanges()))));
        });
  }

  /**
   * Drops an ontology.
   *
   * @param metalake The containing metalake.
   * @param ontology The ontology name.
   * @return The REST response.
   */
  @DELETE
  @Path("{ontology}")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.DROP_ONTOLOGY_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.ONTOLOGY)
  public Response drop(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return Utils.ok(
              new DropResponse(
                  dispatcher.dropOntology(
                      metalake, ontology, OntologyRestSupport::canLoadMapping)));
        });
  }

  /**
   * Exports an ontology as a standalone Ossie document.
   *
   * @param metalake The containing metalake.
   * @param ontology The ontology name.
   * @param format The output format.
   * @return The REST response.
   */
  @GET
  @Path("{ontology}/ossie")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_ONTOLOGY_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.ONTOLOGY)
  @Produces({MediaType.APPLICATION_JSON, YAML})
  public Response exportDocument(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology,
      @DefaultValue("yaml") @QueryParam("format") String format) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return OntologyRestSupport.exported(
              dispatcher.exportOntology(metalake, ontology, OntologyRestSupport.format(format)));
        });
  }

  /**
   * Lists the ontology mappings readable by the caller.
   *
   * @param metalake The containing metalake.
   * @param ontology The ontology name.
   * @return The REST response.
   */
  @GET
  @Path("{ontology}/mappings")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_ONTOLOGY_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.ONTOLOGY)
  public Response listMappings(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("ontology") @AuthorizationMetadata(type = Entity.EntityType.ONTOLOGY)
          String ontology) {
    return OntologyRestSupport.run(
        request,
        () -> {
          NameIdentifier[] visible =
              dispatcher
                  .listOntologyMappings(metalake, ontology, OntologyRestSupport::canLoadMapping)
                  .toArray(NameIdentifier[]::new);
          return Utils.ok(new EntityListResponse(visible));
        });
  }
}
