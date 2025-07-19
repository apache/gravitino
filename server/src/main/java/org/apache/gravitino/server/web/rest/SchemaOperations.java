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
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.Schema;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.SchemaDispatcher;
import org.apache.gravitino.dto.requests.SchemaCreateRequest;
import org.apache.gravitino.dto.requests.SchemaUpdateRequest;
import org.apache.gravitino.dto.requests.SchemaUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.SchemaResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataFilterHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs/{catalog}/schemas")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class SchemaOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaOperations.class);

  private static final String loadSchemaAuthorizationExpression =
      " ANY(OWNER, METALAKE, CATALOG) || "
          + "ANY_USE_CATALOG && (SCHEMA::OWNER || ANY_USE_SCHEMA) ";

  private final SchemaDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public SchemaOperations(SchemaDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-schema", absolute = true)
  public Response listSchemas(
      @PathParam("metalake") String metalake, @PathParam("catalog") String catalog) {
    LOG.info("Received list schema request for catalog: {}.{}", metalake, catalog);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace schemaNS = NamespaceUtil.ofSchema(metalake, catalog);
            NameIdentifier[] idents = dispatcher.listSchemas(schemaNS);
            idents =
                MetadataFilterHelper.filterByExpression(
                    metalake, loadSchemaAuthorizationExpression, Entity.EntityType.SCHEMA, idents);
            Response response = Utils.ok(new EntityListResponse(idents));
            LOG.info("List {} schemas in catalog {}.{}", idents.length, metalake, catalog);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.LIST, "", catalog, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-schema", absolute = true)
  @AuthorizationExpression(
      expression = "ANY(OWNER, METALAKE, CATALOG) || ANY_USE_CATALOG && ANY_CREATE_SCHEMA",
      accessMetadataType = MetadataObject.Type.CATALOG)
  public Response createSchema(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      SchemaCreateRequest request) {
    LOG.info("Received create schema request: {}.{}.{}", metalake, catalog, request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifierUtil.ofSchema(metalake, catalog, request.getName());
            Schema schema =
                dispatcher.createSchema(ident, request.getComment(), request.getProperties());
            Response response = Utils.ok(new SchemaResponse(DTOConverters.toDTO(schema)));
            LOG.info("Schema created: {}.{}.{}", metalake, catalog, schema.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(
          OperationType.CREATE, request.getName(), catalog, e);
    }
  }

  @GET
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-schema", absolute = true)
  @AuthorizationExpression(
      expression = loadSchemaAuthorizationExpression,
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response loadSchema(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema) {
    LOG.info("Received load schema request for schema: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofSchema(metalake, catalog, schema);
            Schema s = dispatcher.loadSchema(ident);
            Response response = Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));
            LOG.info("Schema loaded: {}.{}.{}", metalake, catalog, s.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.LOAD, schema, catalog, e);
    }
  }

  @PUT
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-schema", absolute = true)
  @AuthorizationExpression(
      expression = "ANY(OWNER, METALAKE, CATALOG) || SCHEMA_OWNER_WITH_USE_CATALOG",
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response alterSchema(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      SchemaUpdatesRequest request) {
    LOG.info("Received alter schema request: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofSchema(metalake, catalog, schema);
            SchemaChange[] changes =
                request.getUpdates().stream()
                    .map(SchemaUpdateRequest::schemaChange)
                    .toArray(SchemaChange[]::new);
            Schema s = dispatcher.alterSchema(ident, changes);
            Response response = Utils.ok(new SchemaResponse(DTOConverters.toDTO(s)));
            LOG.info("Schema altered: {}.{}.{}", metalake, catalog, s.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.ALTER, schema, catalog, e);
    }
  }

  @DELETE
  @Path("/{schema}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-schema", absolute = true)
  @AuthorizationExpression(
      expression = "ANY(OWNER, METALAKE, CATALOG) || SCHEMA_OWNER_WITH_USE_CATALOG",
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response dropSchema(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @DefaultValue("false") @QueryParam("cascade") boolean cascade) {
    LOG.info("Received drop schema request: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofSchema(metalake, catalog, schema);
            boolean dropped = dispatcher.dropSchema(ident, cascade);
            if (!dropped) {
              LOG.warn("Fail to drop schema {} under namespace {}", schema, ident.namespace());
            }
            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Schema dropped: {}.{}.{}", metalake, catalog, schema);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleSchemaException(OperationType.DROP, schema, catalog, e);
    }
  }
}
