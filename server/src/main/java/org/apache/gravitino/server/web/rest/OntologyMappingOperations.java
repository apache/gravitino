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
import org.apache.gravitino.dto.ontology.OntologyMappingDTO;
import org.apache.gravitino.dto.requests.OntologyMappingCreateRequest;
import org.apache.gravitino.dto.requests.OntologyUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.OntologyMappingResponse;
import org.apache.gravitino.ontology.OntologyDispatcher;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;

/** REST endpoints for ontology mappings scoped to a semantic model. */
@Path(
    "/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models/{semanticModel}/ontology-mappings")
@Consumes({VND_GRAVITINO_V1_JSON, MediaType.APPLICATION_JSON})
@Produces(VND_GRAVITINO_V1_JSON)
public class OntologyMappingOperations {

  private final OntologyDispatcher dispatcher;
  @Context private HttpServletRequest request;

  /**
   * Constructs the resource.
   *
   * @param dispatcher The ontology dispatcher.
   */
  @Inject
  public OntologyMappingOperations(OntologyDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * Lists visible mappings.
   *
   * @param metalake The containing metalake.
   * @param catalog The containing catalog.
   * @param schema The containing schema.
   * @param semanticModel The parent semantic model.
   * @return The REST response.
   */
  @GET
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SEMANTIC_MODEL)
  public Response list(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("semanticModel") @AuthorizationMetadata(type = Entity.EntityType.SEMANTIC_MODEL)
          String semanticModel) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return Utils.ok(
              new EntityListResponse(
                  dispatcher
                      .listModelMappings(
                          NameIdentifierUtil.ofSemanticModel(
                              metalake, catalog, schema, semanticModel),
                          OntologyRestSupport::canModifyModel)
                      .toArray(NameIdentifier[]::new)));
        });
  }

  /**
   * Creates a mapping.
   *
   * @param metalake The containing metalake.
   * @param catalog The containing catalog.
   * @param schema The containing schema.
   * @param semanticModel The parent semantic model.
   * @param body The native create request.
   * @return The REST response.
   */
  @POST
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SEMANTIC_MODEL)
  public Response create(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("semanticModel") @AuthorizationMetadata(type = Entity.EntityType.SEMANTIC_MODEL)
          String semanticModel,
      OntologyMappingCreateRequest body) {
    return OntologyRestSupport.run(
        request,
        () -> {
          OntologyRestSupport.validate(body);
          return Utils.ok(
              new OntologyMappingResponse(
                  OntologyMappingDTO.from(
                      dispatcher.createMapping(
                          NameIdentifierUtil.ofSemanticModel(
                              metalake, catalog, schema, semanticModel),
                          body.name(),
                          body.comment(),
                          body.ontology(),
                          body.definition(),
                          body.properties(),
                          name -> OntologyRestSupport.requireOntologyLoad(metalake, name)))));
        });
  }

  /**
   * Imports a standalone Ossie mapping.
   *
   * @param metalake The containing metalake.
   * @param catalog The containing catalog.
   * @param schema The containing schema.
   * @param semanticModel The parent semantic model.
   * @param document The raw Ossie input.
   * @param headers The input media type.
   * @return The REST response.
   */
  @POST
  @Path("ossie")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SEMANTIC_MODEL)
  @Consumes({MediaType.APPLICATION_JSON, YAML, "application/x-yaml", "text/yaml"})
  public Response importDocument(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("semanticModel") @AuthorizationMetadata(type = Entity.EntityType.SEMANTIC_MODEL)
          String semanticModel,
      String document,
      @Context HttpHeaders headers) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return Utils.ok(
              new OntologyMappingResponse(
                  OntologyMappingDTO.from(
                      dispatcher.importMapping(
                          NameIdentifierUtil.ofSemanticModel(
                              metalake, catalog, schema, semanticModel),
                          OntologyRestSupport.document(document, headers),
                          name -> OntologyRestSupport.requireOntologyLoad(metalake, name)))));
        });
  }

  /**
   * Loads a mapping.
   *
   * @param metalake The containing metalake.
   * @param catalog The containing catalog.
   * @param schema The containing schema.
   * @param semanticModel The parent semantic model.
   * @param mapping The mapping name.
   * @return The REST response.
   */
  @GET
  @Path("{mapping}")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SEMANTIC_MODEL)
  public Response load(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("semanticModel") @AuthorizationMetadata(type = Entity.EntityType.SEMANTIC_MODEL)
          String semanticModel,
      @PathParam("mapping") String mapping) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return Utils.ok(
              new OntologyMappingResponse(
                  OntologyMappingDTO.from(
                      dispatcher.loadMapping(
                          NameIdentifierUtil.ofSemanticModel(
                              metalake, catalog, schema, semanticModel),
                          mapping,
                          OntologyRestSupport::canModifyModel))));
        });
  }

  /**
   * Alters a mapping.
   *
   * @param metalake The containing metalake.
   * @param catalog The containing catalog.
   * @param schema The containing schema.
   * @param semanticModel The parent semantic model.
   * @param mapping The mapping name.
   * @param body The ordered changes.
   * @return The REST response.
   */
  @PUT
  @Path("{mapping}")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SEMANTIC_MODEL)
  public Response alter(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("semanticModel") @AuthorizationMetadata(type = Entity.EntityType.SEMANTIC_MODEL)
          String semanticModel,
      @PathParam("mapping") String mapping,
      OntologyUpdatesRequest body) {
    return OntologyRestSupport.run(
        request,
        () -> {
          OntologyRestSupport.validate(body);
          return Utils.ok(
              new OntologyMappingResponse(
                  OntologyMappingDTO.from(
                      dispatcher.alterMapping(
                          NameIdentifierUtil.ofSemanticModel(
                              metalake, catalog, schema, semanticModel),
                          mapping,
                          name -> OntologyRestSupport.requireOntologyLoad(metalake, name),
                          body.mappingChanges()))));
        });
  }

  /**
   * Drops a mapping.
   *
   * @param metalake The containing metalake.
   * @param catalog The containing catalog.
   * @param schema The containing schema.
   * @param semanticModel The parent semantic model.
   * @param mapping The mapping name.
   * @return The REST response.
   */
  @DELETE
  @Path("{mapping}")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.MODIFY_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SEMANTIC_MODEL)
  public Response drop(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("semanticModel") @AuthorizationMetadata(type = Entity.EntityType.SEMANTIC_MODEL)
          String semanticModel,
      @PathParam("mapping") String mapping) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return Utils.ok(
              new DropResponse(
                  dispatcher.dropMapping(
                      NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, semanticModel),
                      mapping)));
        });
  }

  /**
   * Exports a mapping as a standalone Ossie document.
   *
   * @param metalake The containing metalake.
   * @param catalog The containing catalog.
   * @param schema The containing schema.
   * @param semanticModel The parent semantic model.
   * @param mapping The mapping name.
   * @param format The output format.
   * @return The REST response.
   */
  @GET
  @Path("{mapping}/ossie")
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SEMANTIC_MODEL_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SEMANTIC_MODEL)
  @Produces({MediaType.APPLICATION_JSON, YAML})
  public Response exportDocument(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("semanticModel") @AuthorizationMetadata(type = Entity.EntityType.SEMANTIC_MODEL)
          String semanticModel,
      @PathParam("mapping") String mapping,
      @DefaultValue("yaml") @QueryParam("format") String format) {
    return OntologyRestSupport.run(
        request,
        () -> {
          return OntologyRestSupport.exported(
              dispatcher.exportMapping(
                  NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, semanticModel),
                  mapping,
                  OntologyRestSupport.format(format),
                  OntologyRestSupport::canModifyModel));
        });
  }
}
