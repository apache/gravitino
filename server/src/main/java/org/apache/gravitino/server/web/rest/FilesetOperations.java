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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.audit.CallerContext;
import org.apache.gravitino.catalog.FilesetDispatcher;
import org.apache.gravitino.dto.requests.FilesetCreateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdateRequest;
import org.apache.gravitino.dto.requests.FilesetUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.FileInfoListResponse;
import org.apache.gravitino.dto.responses.FileLocationResponse;
import org.apache.gravitino.dto.responses.FilesetResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.file.FilesetChange;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.rest.RESTUtils;
import org.apache.gravitino.server.authorization.MetadataFilterHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/filesets")
public class FilesetOperations {

  private static final Logger LOG = LoggerFactory.getLogger(FilesetOperations.class);

  private final FilesetDispatcher dispatcher;

  private static final String loadFilesetAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG) || "
          + "SCHEMA_OWNER_WITH_USE_CATALOG || "
          + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (FILESET::OWNER || ANY_READ_FILESET || ANY_WRITE_FILESET)";

  @Context private HttpServletRequest httpRequest;

  @Inject
  public FilesetOperations(FilesetDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-fileset", absolute = true)
  public Response listFilesets(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema) {

    try {
      LOG.info("Received list filesets request for schema: {}.{}.{}", metalake, catalog, schema);
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace filesetNS = NamespaceUtil.ofFileset(metalake, catalog, schema);
            NameIdentifier[] idents = dispatcher.listFilesets(filesetNS);
            idents =
                MetadataFilterHelper.filterByExpression(
                    metalake,
                    loadFilesetAuthorizationExpression,
                    Entity.EntityType.FILESET,
                    idents);
            Response response = Utils.ok(new EntityListResponse(idents));
            LOG.info(
                "List {} filesets under schema: {}.{}.{}",
                idents.length,
                metalake,
                catalog,
                schema);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-fileset", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_CREATE_FILESET",
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response createFileset(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      FilesetCreateRequest request) {
    LOG.info(
        "Received create fileset request: {}.{}.{}.{}",
        metalake,
        catalog,
        schema,
        request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifierUtil.ofFileset(metalake, catalog, schema, request.getName());

            // set storageLocation value as unnamed location if provided
            Map<String, String> tmpLocations =
                new HashMap<>(
                    Optional.ofNullable(request.getStorageLocations())
                        .orElse(Collections.emptyMap()));
            Optional.ofNullable(request.getStorageLocation())
                .ifPresent(loc -> tmpLocations.put(LOCATION_NAME_UNKNOWN, loc));
            Map<String, String> storageLocations = ImmutableMap.copyOf(tmpLocations);

            Fileset fileset =
                dispatcher.createMultipleLocationFileset(
                    ident,
                    request.getComment(),
                    Optional.ofNullable(request.getType()).orElse(Fileset.Type.MANAGED),
                    storageLocations,
                    request.getProperties());
            Response response = Utils.ok(new FilesetResponse(DTOConverters.toDTO(fileset)));
            LOG.info("Fileset created: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(
          OperationType.CREATE, request.getName(), schema, e);
    }
  }

  @GET
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-fileset", absolute = true)
  @AuthorizationExpression(
      expression = loadFilesetAuthorizationExpression,
      accessMetadataType = MetadataObject.Type.FILESET)
  public Response loadFileset(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("fileset") @AuthorizationMetadata(type = Entity.EntityType.FILESET)
          String fileset) {
    LOG.info("Received load fileset request: {}.{}.{}.{}", metalake, catalog, schema, fileset);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            Fileset t = dispatcher.loadFileset(ident);
            Response response = Utils.ok(new FilesetResponse(DTOConverters.toDTO(t)));
            LOG.info("Fileset loaded: {}.{}.{}.{}", metalake, catalog, schema, fileset);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LOAD, fileset, schema, e);
    }
  }

  @GET
  @Path("{fileset}/files")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-fileset-files." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-fileset-files", absolute = true)
  @AuthorizationExpression(
      expression = loadFilesetAuthorizationExpression,
      accessMetadataType = MetadataObject.Type.FILESET)
  public Response listFiles(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("fileset") @AuthorizationMetadata(type = Entity.EntityType.FILESET) String fileset,
      @QueryParam("sub_path") @DefaultValue("/") String subPath,
      @QueryParam("location_name") String locationName) {
    LOG.info(
        "Received list files request: {}.{}.{}.{}, subPath: {}, locationName:{}",
        metalake,
        catalog,
        schema,
        fileset,
        subPath,
        locationName);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            int[] clientVersion = Utils.getClientVersion(httpRequest);
            boolean isV1PlusClient = clientVersion == null || clientVersion[0] >= 1;
            String decodedSubPath = isV1PlusClient ? subPath : RESTUtils.decodeString(subPath);

            NameIdentifier filesetIdent =
                NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            FileInfo[] files = dispatcher.listFiles(filesetIdent, locationName, decodedSubPath);
            Response response = Utils.ok(new FileInfoListResponse(DTOConverters.toDTO(files)));
            LOG.info(
                "Files listed for fileset: {}.{}.{}.{}, subPath: {}, locationName:{}",
                metalake,
                catalog,
                schema,
                fileset,
                decodedSubPath,
                locationName);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.LOAD, fileset, schema, e);
    }
  }

  @PUT
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-fileset", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && (FILESET::OWNER || ANY_WRITE_FILESET)",
      accessMetadataType = MetadataObject.Type.FILESET)
  public Response alterFileset(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("fileset") @AuthorizationMetadata(type = Entity.EntityType.FILESET) String fileset,
      FilesetUpdatesRequest request) {
    LOG.info("Received alter fileset request: {}.{}.{}.{}", metalake, catalog, schema, fileset);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            FilesetChange[] changes =
                request.getUpdates().stream()
                    .map(FilesetUpdateRequest::filesetChange)
                    .toArray(FilesetChange[]::new);
            Fileset t = dispatcher.alterFileset(ident, changes);
            Response response = Utils.ok(new FilesetResponse(DTOConverters.toDTO(t)));
            LOG.info("Fileset altered: {}.{}.{}.{}", metalake, catalog, schema, t.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.ALTER, fileset, schema, e);
    }
  }

  @DELETE
  @Path("{fileset}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-fileset." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-fileset", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && FILESET::OWNER",
      accessMetadataType = MetadataObject.Type.FILESET)
  public Response dropFileset(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("fileset") @AuthorizationMetadata(type = Entity.EntityType.FILESET)
          String fileset) {
    LOG.info("Received drop fileset request: {}.{}.{}.{}", metalake, catalog, schema, fileset);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            boolean dropped = dispatcher.dropFileset(ident);
            if (!dropped) {
              LOG.warn("Cannot find to be dropped fileset {} under schema {}", fileset, schema);
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info("Fileset dropped: {}.{}.{}.{}", metalake, catalog, schema, fileset);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.DROP, fileset, schema, e);
    }
  }

  @GET
  @Path("{fileset}/location")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "get-file-location." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-file-location", absolute = true)
  @AuthorizationExpression(
      expression = loadFilesetAuthorizationExpression,
      accessMetadataType = MetadataObject.Type.FILESET)
  public Response getFileLocation(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("fileset") @AuthorizationMetadata(type = Entity.EntityType.FILESET) String fileset,
      @QueryParam("sub_path") @NotNull String subPath,
      @QueryParam("location_name") String locationName) {
    LOG.info(
        "Received get file location request: {}.{}.{}.{}, sub path:{}, location name:{}",
        metalake,
        catalog,
        schema,
        fileset,
        subPath,
        locationName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            int[] clientVersion = Utils.getClientVersion(httpRequest);
            boolean isV1PlusClient = clientVersion == null || clientVersion[0] >= 1;
            String decodedSubPath = isV1PlusClient ? subPath : RESTUtils.decodeString(subPath);
            String decodedLocationName =
                isV1PlusClient
                    ? locationName
                    : Optional.ofNullable(locationName).map(RESTUtils::decodeString).orElse(null);

            NameIdentifier ident = NameIdentifierUtil.ofFileset(metalake, catalog, schema, fileset);
            Map<String, String> filteredAuditHeaders = Utils.filterFilesetAuditHeaders(httpRequest);
            // set the audit info into the thread local context
            if (!filteredAuditHeaders.isEmpty()) {
              CallerContext context =
                  CallerContext.builder().withContext(filteredAuditHeaders).build();
              CallerContext.CallerContextHolder.set(context);
            }
            String actualFileLocation =
                dispatcher.getFileLocation(ident, decodedSubPath, decodedLocationName);
            return Utils.ok(new FileLocationResponse(actualFileLocation));
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleFilesetException(OperationType.GET, fileset, schema, e);
    } finally {
      // Clear the caller context
      CallerContext.CallerContextHolder.remove();
    }
  }
}
