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
import java.util.Collections;
import java.util.Locale;
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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.SemanticModelDispatcher;
import org.apache.gravitino.dto.requests.SemanticModelCreateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdateRequest;
import org.apache.gravitino.dto.requests.SemanticModelUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.SemanticModelResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.semantic.OssieSemanticModelDocumentConverter;
import org.apache.gravitino.semantic.SemanticModel;
import org.apache.gravitino.semantic.SemanticModelChange;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** REST operations for schema-scoped Semantic Model management. */
@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/semantic-models")
public class SemanticModelOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SemanticModelOperations.class);
  private static final String VND_GRAVITINO_V1_JSON = "application/vnd.gravitino.v1+json";
  private static final String OSSIE_YAML_MEDIA_TYPE = "application/yaml";
  private static final String OSSIE_X_YAML_MEDIA_TYPE = "application/x-yaml";

  private final SemanticModelDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  /**
   * Creates Semantic Model REST operations.
   *
   * @param dispatcher The Semantic Model dispatcher.
   */
  @Inject
  public SemanticModelOperations(SemanticModelDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  /**
   * Lists Semantic Models in a schema.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @return A response containing Semantic Model identifiers.
   */
  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-semantic-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-semantic-model", absolute = true)
  public Response listSemanticModels(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    LOG.info(
        "Received list Semantic Models request for schema: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace namespace = NamespaceUtil.ofSemanticModel(metalake, catalog, schema);
            NameIdentifier[] identifiers = dispatcher.listSemanticModels(namespace);
            LOG.info(
                "List {} Semantic Models under schema: {}.{}.{}",
                identifiers.length,
                metalake,
                catalog,
                schema);
            return Utils.ok(new EntityListResponse(identifiers));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(OperationType.LIST, "", schema, e);
    }
  }

  /**
   * Creates a Semantic Model.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param request The structured Semantic Model create request.
   * @return A response containing the created Semantic Model.
   */
  @POST
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "create-semantic-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-semantic-model", absolute = true)
  public Response createSemanticModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      SemanticModelCreateRequest request) {
    String name = request == null ? "" : request.getName();
    LOG.info(
        "Received create Semantic Model request: {}.{}.{}.{}", metalake, catalog, schema, name);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (request == null) {
              throw new IllegalArgumentException("Request body must not be null");
            }
            SemanticModel semanticModel =
                createSemanticModelEntity(metalake, catalog, schema, request);
            LOG.info(
                "Semantic Model created: {}.{}.{}.{}",
                metalake,
                catalog,
                schema,
                semanticModel.name());
            return Utils.ok(new SemanticModelResponse(DTOConverters.toDTO(semanticModel)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(OperationType.CREATE, name, schema, e);
    }
  }

  /**
   * Imports a standalone Apache Ossie YAML or JSON document as a Semantic Model.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param document The standalone Ossie document.
   * @return A native response containing the created Semantic Model.
   */
  @POST
  @Path("ossie")
  @Consumes({MediaType.APPLICATION_JSON, OSSIE_YAML_MEDIA_TYPE, OSSIE_X_YAML_MEDIA_TYPE})
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "import-ossie-semantic-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "import-ossie-semantic-model", absolute = true)
  public Response importOssieSemanticModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      String document) {
    LOG.info(
        "Received import Apache Ossie Semantic Model request for schema: {}.{}.{}",
        metalake,
        catalog,
        schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            SemanticModelCreateRequest request =
                OssieSemanticModelDocumentConverter.importDocument(document);
            SemanticModel semanticModel =
                createSemanticModelEntity(metalake, catalog, schema, request);
            LOG.info(
                "Apache Ossie Semantic Model imported: {}.{}.{}.{}",
                metalake,
                catalog,
                schema,
                semanticModel.name());
            return Utils.ok(new SemanticModelResponse(DTOConverters.toDTO(semanticModel)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(
          OperationType.CREATE, "Apache Ossie document", schema, e);
    }
  }

  /**
   * Loads a Semantic Model.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param semanticModel The Semantic Model name.
   * @return A response containing the loaded Semantic Model.
   */
  @GET
  @Path("{semanticModel}")
  @Produces(VND_GRAVITINO_V1_JSON)
  @Timed(name = "load-semantic-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-semantic-model", absolute = true)
  public Response loadSemanticModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("semanticModel") String semanticModel) {
    LOG.info(
        "Received load Semantic Model request: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        semanticModel);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident =
                NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, semanticModel);
            SemanticModel loaded = dispatcher.loadSemanticModel(ident);
            LOG.info(
                "Semantic Model loaded: {}.{}.{}.{}", metalake, catalog, schema, semanticModel);
            return Utils.ok(new SemanticModelResponse(DTOConverters.toDTO(loaded)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(
          OperationType.LOAD, semanticModel, schema, e);
    }
  }

  /**
   * Alters a Semantic Model atomically.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param semanticModel The current Semantic Model name.
   * @param request The updates to apply.
   * @return A response containing the altered Semantic Model.
   */
  @PUT
  @Path("{semanticModel}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-semantic-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-semantic-model", absolute = true)
  public Response alterSemanticModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("semanticModel") String semanticModel,
      SemanticModelUpdatesRequest request) {
    LOG.info(
        "Received alter Semantic Model request: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        semanticModel);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            if (request == null) {
              throw new IllegalArgumentException("Request body must not be null");
            }
            request.validate();
            NameIdentifier ident =
                NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, semanticModel);
            SemanticModelChange[] changes =
                request.getUpdates().stream()
                    .map(SemanticModelUpdateRequest::semanticModelChange)
                    .toArray(SemanticModelChange[]::new);
            SemanticModel altered = dispatcher.alterSemanticModel(ident, changes);
            LOG.info(
                "Semantic Model altered: {}.{}.{}.{}", metalake, catalog, schema, altered.name());
            return Utils.ok(new SemanticModelResponse(DTOConverters.toDTO(altered)));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(
          OperationType.ALTER, semanticModel, schema, e);
    }
  }

  /**
   * Drops a Semantic Model.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param semanticModel The Semantic Model name.
   * @return A response indicating whether a Semantic Model was dropped.
   */
  @DELETE
  @Path("{semanticModel}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-semantic-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-semantic-model", absolute = true)
  public Response dropSemanticModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("semanticModel") String semanticModel) {
    LOG.info(
        "Received drop Semantic Model request: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        semanticModel);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident =
                NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, semanticModel);
            boolean dropped = dispatcher.dropSemanticModel(ident);
            if (dropped) {
              LOG.info(
                  "Semantic Model dropped: {}.{}.{}.{}", metalake, catalog, schema, semanticModel);
            } else {
              LOG.warn(
                  "Cannot find Semantic Model {} to drop under schema {}", semanticModel, schema);
            }
            return Utils.ok(new DropResponse(dropped));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(
          OperationType.DROP, semanticModel, schema, e);
    }
  }

  /**
   * Exports a Semantic Model as a standalone Apache Ossie YAML or JSON document.
   *
   * @param metalake The metalake name.
   * @param catalog The catalog name.
   * @param schema The schema name.
   * @param semanticModel The Semantic Model name.
   * @param format The output format, either {@code yaml} or {@code json}.
   * @return A response containing the serialized Ossie document.
   */
  @GET
  @Path("{semanticModel}/ossie")
  @Timed(name = "export-ossie-semantic-model." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "export-ossie-semantic-model", absolute = true)
  public Response exportOssieSemanticModel(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema,
      @PathParam("semanticModel") String semanticModel,
      @DefaultValue("yaml") @QueryParam("format") String format) {
    LOG.info(
        "Received export Semantic Model as Apache Ossie request: {}.{}.{}.{}, format: {}",
        metalake,
        catalog,
        schema,
        semanticModel,
        format);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            OssieSemanticModelDocumentConverter.Format outputFormat = parseOssieFormat(format);
            NameIdentifier ident =
                NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, semanticModel);
            SemanticModel loaded = dispatcher.loadSemanticModel(ident);
            String document =
                OssieSemanticModelDocumentConverter.exportDocument(loaded, outputFormat);
            String mediaType =
                outputFormat == OssieSemanticModelDocumentConverter.Format.JSON
                    ? MediaType.APPLICATION_JSON
                    : OSSIE_YAML_MEDIA_TYPE;
            return Response.ok(document, mediaType)
                .header("Content-Disposition", ossieContentDisposition(semanticModel, outputFormat))
                .build();
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSemanticModelException(
          OperationType.LOAD, semanticModel, schema, e);
    }
  }

  private SemanticModel createSemanticModelEntity(
      String metalake, String catalog, String schema, SemanticModelCreateRequest request) {
    request.validate();
    SemanticModelDefinition definition = request.toDefinition();
    NameIdentifier ident =
        NameIdentifierUtil.ofSemanticModel(metalake, catalog, schema, request.getName());
    return dispatcher.createSemanticModel(
        ident,
        request.getComment(),
        definition,
        request.getProperties() == null ? Collections.emptyMap() : request.getProperties());
  }

  private static OssieSemanticModelDocumentConverter.Format parseOssieFormat(String format) {
    if (format == null) {
      throw new IllegalArgumentException("Ossie format must be yaml or json");
    }
    try {
      return OssieSemanticModelDocumentConverter.Format.valueOf(format.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Unsupported Ossie format '%s'; expected yaml or json", format), e);
    }
  }

  private static String ossieContentDisposition(
      String semanticModel, OssieSemanticModelDocumentConverter.Format format) {
    String safeName = semanticModel.replaceAll("[^A-Za-z0-9._-]", "_");
    String extension = format.name().toLowerCase(Locale.ROOT);
    return String.format("attachment; filename=\"%s.ossie.%s\"", safeName, extension);
  }
}
