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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;
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
import org.apache.gravitino.Catalog;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.Entity;
import org.apache.gravitino.GravitinoEnv;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.CatalogDispatcher;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.dto.CatalogDTO;
import org.apache.gravitino.dto.requests.CatalogCreateRequest;
import org.apache.gravitino.dto.requests.CatalogSetRequest;
import org.apache.gravitino.dto.requests.CatalogUpdateRequest;
import org.apache.gravitino.dto.requests.CatalogUpdatesRequest;
import org.apache.gravitino.dto.responses.BaseResponse;
import org.apache.gravitino.dto.responses.CatalogListResponse;
import org.apache.gravitino.dto.responses.CatalogResponse;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/metalakes/{metalake}/catalogs")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class CatalogOperations {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogOperations.class);

  private final CatalogDispatcher catalogDispatcher;

  private final boolean filterSensitiveProperties;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public CatalogOperations(CatalogDispatcher catalogDispatcher) {
    this.catalogDispatcher = catalogDispatcher;
    this.filterSensitiveProperties = GravitinoEnv.getInstance().filterSensitiveProperties();
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-catalog." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-catalog", absolute = true)
  @AuthorizationExpression(expression = "")
  public Response listCatalogs(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @QueryParam("details") @DefaultValue("false") boolean verbose) {
    LOG.info(
        "Received list catalog {} request for metalake: {}, ",
        verbose ? "infos" : "names",
        metalake);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace catalogNS = NamespaceUtil.ofCatalog(metalake);
            // Lock the root and the metalake with WRITE lock to ensure the consistency of the list.
            if (verbose) {
              Catalog[] catalogs = catalogDispatcher.listCatalogsInfo(catalogNS);
              CatalogDTO[] catalogDTOs;
              if (filterSensitiveProperties) {
                MetadataAuthzHelper.FilterResult<Catalog, Catalog> filterResult =
                    MetadataAuthzHelper.partitionByTwoExpressions(
                        metalake,
                        AuthorizationExpressionConstants.CATALOG_OWNER_EXPRESSION,
                        AuthorizationExpressionConstants.USE_CATALOG_EXPRESSION,
                        Entity.EntityType.CATALOG,
                        catalogs,
                        (catalogEntity) ->
                            NameIdentifierUtil.ofCatalog(metalake, catalogEntity.name()));
                // First array: catalogs with full access (can see sensitive properties)
                CatalogDTO[] fullAccessCatalogs = DTOConverters.toDTOs(filterResult.getFirst());
                // Second array: catalogs with use access only (hide sensitive properties)
                CatalogDTO[] limitedAccessCatalogs =
                    buildCatalogDTOsWithoutSensitiveProps(filterResult.getSecond());
                catalogDTOs =
                    Stream.concat(
                            Arrays.stream(fullAccessCatalogs), Arrays.stream(limitedAccessCatalogs))
                        .toArray(CatalogDTO[]::new);
              } else {
                // If filtering is disabled, return all catalogs with full properties
                catalogDTOs = DTOConverters.toDTOs(catalogs);
              }
              Response response = Utils.ok(new CatalogListResponse(catalogDTOs));
              LOG.info("List {} catalogs info under metalake: {}", catalogDTOs.length, metalake);
              return response;
            } else {
              NameIdentifier[] idents = catalogDispatcher.listCatalogs(catalogNS);
              idents =
                  MetadataAuthzHelper.filterByExpression(
                      metalake,
                      AuthorizationExpressionConstants.LOAD_CATALOG_AUTHORIZATION_EXPRESSION,
                      Entity.EntityType.CATALOG,
                      idents);
              Response response = Utils.ok(new EntityListResponse(idents));
              LOG.info("List {} catalogs under metalake: {}", idents.length, metalake);
              return response;
            }
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(OperationType.LIST, "", metalake, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-catalog." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-catalog", absolute = true)
  @AuthorizationExpression(
      expression = "METALAKE::CREATE_CATALOG || METALAKE::OWNER",
      accessMetadataType = MetadataObject.Type.METALAKE)
  public Response createCatalog(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      CatalogCreateRequest request) {
    LOG.info("Received create catalog request for metalake: {}", metalake);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalake, request.getName());
            Catalog catalog =
                catalogDispatcher.createCatalog(
                    ident,
                    request.getType(),
                    request.getProvider(),
                    request.getComment(),
                    request.getProperties());
            Response response = Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));
            LOG.info("Catalog created: {}.{}", metalake, catalog.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.CREATE, request.getName(), metalake, e);
    }
  }

  @POST
  @Path("testConnection")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "test-connection." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @AuthorizationExpression(
      expression = "METALAKE::CREATE_CATALOG || METALAKE::OWNER",
      accessMetadataType = MetadataObject.Type.METALAKE)
  @ResponseMetered(name = "test-connection", absolute = true)
  public Response testConnection(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      CatalogCreateRequest request) {
    LOG.info("Received test connection request for catalog: {}.{}", metalake, request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalake, request.getName());
            catalogDispatcher.testConnection(
                ident,
                request.getType(),
                request.getProvider(),
                request.getComment(),
                request.getProperties());
            Response response = Utils.ok(new BaseResponse());
            LOG.info(
                "Successfully test connection for catalog: {}.{}", metalake, request.getName());
            return response;
          });

    } catch (Exception e) {
      LOG.info("Failed to test connection for catalog: {}.{}", metalake, request.getName());
      return ExceptionHandlers.handleTestConnectionException(e);
    }
  }

  @PATCH
  @Path("{catalog}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "set-catalog." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "set-catalog", absolute = true)
  @AuthorizationExpression(
      expression = "ANY_USE_CATALOG || ANY(OWNER, METALAKE, CATALOG)",
      accessMetadataType = MetadataObject.Type.CATALOG)
  public Response setCatalog(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG)
          String catalogName,
      CatalogSetRequest request) {
    LOG.info("Received set request for catalog: {}.{}", metalake, catalogName);

    OperationType op = request.isInUse() ? OperationType.ENABLE : OperationType.DISABLE;

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalake, catalogName);
            if (op == OperationType.ENABLE) {
              catalogDispatcher.enableCatalog(ident);
            } else {
              catalogDispatcher.disableCatalog(ident);
            }

            Response response = Utils.ok(new BaseResponse());
            LOG.info(
                "Successfully {} catalog: {}.{}",
                request.isInUse() ? "enable" : "disable",
                metalake,
                catalogName);
            return response;
          });

    } catch (Exception e) {
      LOG.info(
          "Failed to {} catalog: {}.{}",
          request.isInUse() ? "enable" : "disable",
          metalake,
          catalogName);
      return ExceptionHandlers.handleCatalogException(op, catalogName, metalake, e);
    }
  }

  @GET
  @Path("{catalog}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-catalog." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-catalog", absolute = true)
  @AuthorizationExpression(
      expression = "ANY_USE_CATALOG || ANY(OWNER, METALAKE, CATALOG)",
      accessMetadataType = MetadataObject.Type.CATALOG)
  public Response loadCatalog(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG)
          String catalogName) {
    LOG.info("Received load catalog request for catalog: {}.{}", metalakeName, catalogName);
    try {
      NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
      Catalog catalog = catalogDispatcher.loadCatalog(ident);
      CatalogDTO catalogDTO;
      if (filterSensitiveProperties) {
        // Only allow users with alter permission to access sensitive data in catalog properties.
        boolean enableAccessSensitiveData =
            MetadataAuthzHelper.checkAccess(
                ident,
                Entity.EntityType.CATALOG,
                AuthorizationExpressionConstants.CATALOG_OWNER_EXPRESSION);
        if (enableAccessSensitiveData) {
          catalogDTO = DTOConverters.toDTO(catalog);
        } else {
          Preconditions.checkState(
              catalog instanceof HasPropertyMetadata,
              "Catalog does not support hiding sensitive properties.");
          PropertiesMetadata propertiesMetadata =
              ((HasPropertyMetadata) catalog).catalogPropertiesMetadata();
          catalogDTO = buildCatalogDTO(catalog, propertiesMetadata);
        }
      } else {
        // If filtering is disabled, return catalog with full properties
        catalogDTO = DTOConverters.toDTO(catalog);
      }
      Response response = Utils.ok(new CatalogResponse(catalogDTO));
      LOG.info("Catalog loaded: {}.{}", metalakeName, catalogName);
      return response;

    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.LOAD, catalogName, metalakeName, e);
    }
  }

  @PUT
  @Path("{catalog}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-catalog." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-catalog", absolute = true)
  @AuthorizationExpression(
      expression = "ANY(OWNER, METALAKE, CATALOG)",
      accessMetadataType = MetadataObject.Type.CATALOG)
  public Response alterCatalog(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG)
          String catalogName,
      CatalogUpdatesRequest request) {
    LOG.info("Received alter catalog request for catalog: {}.{}", metalakeName, catalogName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
            CatalogChange[] changes =
                request.getUpdates().stream()
                    .map(CatalogUpdateRequest::catalogChange)
                    .toArray(CatalogChange[]::new);
            Catalog catalog = catalogDispatcher.alterCatalog(ident, changes);
            Response response = Utils.ok(new CatalogResponse(DTOConverters.toDTO(catalog)));
            LOG.info("Catalog altered: {}.{}", metalakeName, catalog.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.ALTER, catalogName, metalakeName, e);
    }
  }

  @DELETE
  @Path("{catalog}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-catalog." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-catalog", absolute = true)
  @AuthorizationExpression(
      expression = "ANY(OWNER, METALAKE, CATALOG)",
      accessMetadataType = MetadataObject.Type.CATALOG)
  public Response dropCatalog(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalakeName,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG)
          String catalogName,
      @DefaultValue("false") @QueryParam("force") boolean force) {
    LOG.info("Received drop catalog request for catalog: {}.{}", metalakeName, catalogName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofCatalog(metalakeName, catalogName);
            boolean dropped = catalogDispatcher.dropCatalog(ident, force);
            if (!dropped) {
              LOG.warn("Failed to drop catalog {} under metalake {}", catalogName, metalakeName);
            } else {
              LOG.info("Catalog dropped: {}.{}", metalakeName, catalogName);
            }
            Response response = Utils.ok(new DropResponse(dropped));
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleCatalogException(
          OperationType.DROP, catalogName, metalakeName, e);
    }
  }

  private static CatalogDTO buildCatalogDTO(
      Catalog catalog, PropertiesMetadata propertiesMetadata) {
    CatalogDTO catalogDTO;
    Map<String, String> newProps = Maps.newHashMap(catalog.properties());
    newProps.keySet().removeIf(propertiesMetadata::isSensitiveProperty);
    catalogDTO =
        CatalogDTO.builder()
            .withName(catalog.name())
            .withType(catalog.type())
            .withProvider(catalog.provider())
            .withComment(catalog.comment())
            .withProperties(newProps)
            .withAudit(DTOConverters.toDTO(catalog.auditInfo()))
            .build();
    return catalogDTO;
  }

  private static CatalogDTO[] buildCatalogDTOsWithoutSensitiveProps(Catalog[] catalogs) {
    return Arrays.stream(catalogs)
        .map(
            catalog -> {
              Preconditions.checkState(
                  catalog instanceof HasPropertyMetadata,
                  "Catalog does not support hiding sensitive properties.");
              PropertiesMetadata propertiesMetadata =
                  ((HasPropertyMetadata) catalog).catalogPropertiesMetadata();
              return buildCatalogDTO(catalog, propertiesMetadata);
            })
        .toArray(CatalogDTO[]::new);
  }
}
