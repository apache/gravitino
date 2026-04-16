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
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Entity;
import org.apache.gravitino.Entity.EntityType;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.credential.CredentialPrivilege;
import org.apache.gravitino.iceberg.service.IcebergCatalogWrapperManager;
import org.apache.gravitino.iceberg.service.IcebergExceptionMapper;
import org.apache.gravitino.iceberg.service.IcebergObjectMapper;
import org.apache.gravitino.iceberg.service.IcebergRESTUtils;
import org.apache.gravitino.iceberg.service.authorization.IcebergRESTServerContext;
import org.apache.gravitino.iceberg.service.dispatcher.IcebergTableOperationDispatcher;
import org.apache.gravitino.iceberg.service.metrics.IcebergMetricsManager;
import org.apache.gravitino.listener.api.event.IcebergRequestContext;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.server.authorization.MetadataAuthzHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata;
import org.apache.gravitino.server.authorization.annotations.IcebergAuthorizationMetadata.RequestType;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.gravitino.server.web.Utils;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/{prefix:([^/]*/)?}namespaces/{namespace}/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableOperations.class);

  @VisibleForTesting
  public static final String X_ICEBERG_ACCESS_DELEGATION = "X-Iceberg-Access-Delegation";

  @VisibleForTesting public static final String IF_NONE_MATCH = "If-None-Match";

  private IcebergMetricsManager icebergMetricsManager;

  private ObjectMapper icebergObjectMapper;
  private IcebergTableOperationDispatcher tableOperationDispatcher;
  private IcebergCatalogWrapperManager icebergCatalogWrapperManager;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public IcebergTableOperations(
      IcebergMetricsManager icebergMetricsManager,
      IcebergTableOperationDispatcher tableOperationDispatcher,
      IcebergCatalogWrapperManager icebergCatalogWrapperManager) {
    this.icebergMetricsManager = icebergMetricsManager;
    this.tableOperationDispatcher = tableOperationDispatcher;
    this.icebergCatalogWrapperManager = icebergCatalogWrapperManager;
    this.icebergObjectMapper = IcebergObjectMapper.getInstance();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "list-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-table", absolute = true)
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response listTable(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info("List Iceberg tables, catalog: {}, namespace: {}", catalogName, icebergNS);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            ListTablesResponse listTablesResponse =
                tableOperationDispatcher.listTable(context, icebergNS);

            IcebergRESTServerContext authContext = IcebergRESTServerContext.getInstance();
            if (authContext.isAuthorizationEnabled()) {
              listTablesResponse =
                  filterListTablesResponse(
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
  @Timed(name = "create-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-table", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_CREATE_TABLE",
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response createTable(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      CreateTableRequest createTableRequest,
      @HeaderParam(X_ICEBERG_ACCESS_DELEGATION) String accessDelegation) {
    boolean isCredentialVending = isCredentialVending(accessDelegation);
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    LOG.info(
        "Create Iceberg table, catalog: {}, namespace: {}, create table request: {}, "
            + "accessDelegation: {}, isCredentialVending: {}",
        catalogName,
        icebergNS,
        createTableRequest,
        accessDelegation,
        isCredentialVending);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName, isCredentialVending);
            LoadTableResponse loadTableResponse =
                tableOperationDispatcher.createTable(context, icebergNS, createTableRequest);
            return buildResponseWithETag(loadTableResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @POST
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "update-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-table", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_MODIFY_TABLE)",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response updateTable(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = Entity.EntityType.TABLE) @Encoded() @PathParam("table")
          String table,
      UpdateTableRequest updateTableRequest) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String tableName = RESTUtil.decodeString(table);
    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Update Iceberg table, catalog: {}, namespace: {}, table: {}, updateTableRequest: {}",
          catalogName,
          icebergNS,
          table,
          SerializeUpdateTableRequest(updateTableRequest));
    }
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            TableIdentifier tableIdentifier = TableIdentifier.of(icebergNS, tableName);
            LoadTableResponse loadTableResponse =
                tableOperationDispatcher.updateTable(context, tableIdentifier, updateTableRequest);
            return buildResponseWithETag(loadTableResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @DELETE
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "drop-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-table", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && TABLE::OWNER ",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response dropTable(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = Entity.EntityType.TABLE) @Encoded() @PathParam("table")
          String table,
      @DefaultValue("false") @QueryParam("purgeRequested") boolean purgeRequested) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String tableName = RESTUtil.decodeString(table);
    LOG.info(
        "Drop Iceberg table, catalog: {}, namespace: {}, table: {}, purgeRequested: {}",
        catalogName,
        icebergNS,
        tableName,
        purgeRequested);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            TableIdentifier tableIdentifier = TableIdentifier.of(icebergNS, tableName);
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            tableOperationDispatcher.dropTable(context, tableIdentifier, purgeRequested);
            return IcebergRESTUtils.noContent();
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @GET
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "load-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-table", absolute = true)
  // SCHEMA-level authorization; TABLE-specific authorization is handled in LoadTableAuthzHandler
  @AuthorizationExpression(
      expression = AuthorizationExpressionConstants.LOAD_SCHEMA_AUTHORIZATION_EXPRESSION,
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response loadTable(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @IcebergAuthorizationMetadata(type = RequestType.LOAD_TABLE) @Encoded() @PathParam("table")
          String table,
      @DefaultValue(IcebergRESTUtils.SNAPSHOT_ALL) @QueryParam("snapshots") String snapshots,
      @HeaderParam(X_ICEBERG_ACCESS_DELEGATION) String accessDelegation,
      @HeaderParam(IF_NONE_MATCH) String ifNoneMatch) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String tableName = RESTUtil.decodeString(table);
    boolean isCredentialVending = isCredentialVending(accessDelegation);
    LOG.info(
        "Load Iceberg table, catalog: {}, namespace: {}, table: {}, access delegation: {}, "
            + "credential vending: {}",
        catalogName,
        icebergNS,
        tableName,
        accessDelegation,
        isCredentialVending);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            TableIdentifier tableIdentifier = TableIdentifier.of(icebergNS, tableName);
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName, isCredentialVending);

            // Fast path: if client sent If-None-Match, try to resolve ETag without full table load
            if (StringUtils.isNotBlank(ifNoneMatch)) {
              Optional<String> metadataLocation =
                  tableOperationDispatcher.getTableMetadataLocation(context, tableIdentifier);
              if (metadataLocation.isPresent()) {
                Optional<EntityTag> etag = generateETag(metadataLocation.get(), snapshots);
                if (etag.isPresent() && etagMatches(ifNoneMatch, etag.get())) {
                  return Response.notModified(etag.get()).build();
                }
              }
            }

            LoadTableResponse loadTableResponse =
                tableOperationDispatcher.loadTable(context, tableIdentifier);
            Optional<EntityTag> etag =
                generateETag(loadTableResponse.tableMetadata().metadataFileLocation(), snapshots);
            if (etag.isPresent() && etagMatches(ifNoneMatch, etag.get())) {
              return Response.notModified(etag.get()).build();
            }
            if (IcebergRESTUtils.SnapshotMode.REFS.getValue().equals(snapshots)) {
              loadTableResponse = filterSnapshotsByRefs(loadTableResponse);
            }
            return buildResponseWithETag(loadTableResponse, etag);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  @HEAD
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "table-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table-exists", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE || ANY_CREATE_TABLE)",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response tableExists(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = Entity.EntityType.TABLE) @Encoded() @PathParam("table")
          String table) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String tableName = RESTUtil.decodeString(table);
    LOG.info(
        "Check Iceberg table exists, catalog: {}, namespace: {}, table: {}",
        catalogName,
        icebergNS,
        tableName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            TableIdentifier tableIdentifier = TableIdentifier.of(icebergNS, tableName);
            boolean exists = tableOperationDispatcher.tableExists(context, tableIdentifier);
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

  @POST
  @Path("{table}/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "report-table-metrics." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "report-table-metrics", absolute = true)
  public Response reportTableMetrics(
      @PathParam("prefix") String prefix,
      @Encoded() @PathParam("namespace") String namespace,
      @Encoded() @PathParam("table") String table,
      ReportMetricsRequest request) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String tableName = RESTUtil.decodeString(table);
    LOG.info(
        "Report Iceberg table metrics, catalog: {}, namespace: {}, table: {}",
        catalogName,
        icebergNS,
        tableName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            boolean accepted =
                icebergMetricsManager.recordMetric(catalogName, icebergNS, request.report());
            if (accepted) {
              return IcebergRESTUtils.noContent();
            } else {
              return IcebergRESTUtils.errorResponse(
                  new RuntimeException("Metrics service unavailable: queue full or service closed"),
                  Response.Status.SERVICE_UNAVAILABLE.getStatusCode());
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

  private String SerializeUpdateTableRequest(UpdateTableRequest updateTableRequest) {
    try {
      return icebergObjectMapper.writeValueAsString(updateTableRequest);
    } catch (JsonProcessingException e) {
      LOG.warn("Serialize update table request failed", e);
      return updateTableRequest.toString();
    }
  }

  @GET
  @Path("{table}/credentials")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "get-table-credentials." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "get-table-credentials", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response getTableCredentials(
      @AuthorizationMetadata(type = Entity.EntityType.CATALOG) @PathParam("prefix") String prefix,
      @AuthorizationMetadata(type = EntityType.SCHEMA) @Encoded() @PathParam("namespace")
          String namespace,
      @AuthorizationMetadata(type = EntityType.TABLE) @Encoded() @PathParam("table") String table) {
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String tableName = RESTUtil.decodeString(table);
    LOG.info(
        "Get Iceberg table credentials, catalog: {}, namespace: {}, table: {}",
        catalogName,
        icebergNS,
        tableName);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            // Convert Iceberg table identifier to Gravitino NameIdentifier
            TableIdentifier tableIdentifier = TableIdentifier.of(icebergNS, tableName);
            // First check if the table exists
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName);
            // Get credentials using the table operation dispatcher
            LoadCredentialsResponse credentialsResponse =
                tableOperationDispatcher.getTableCredentials(context, tableIdentifier);
            return IcebergRESTUtils.ok(credentialsResponse);
          });
    } catch (Exception e) {
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  /**
   * Plan table scan endpoint. Allows clients to request a scan plan from the server to optimize
   * scan planning by leveraging server-side resources.
   *
   * @param prefix The catalog prefix
   * @param namespace The namespace
   * @param table The table name
   * @param scanRequest The scan request containing filters, projections, etc.
   * @return Response containing the scan plan with tasks
   */
  @POST
  @Path("{table}/scan")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @Timed(name = "plan-table-scan." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "plan-table-scan", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE|| ANY_MODIFY_TABLE)",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response planTableScan(
      @PathParam("prefix") @AuthorizationMetadata(type = EntityType.CATALOG) String prefix,
      @Encoded() @PathParam("namespace") @AuthorizationMetadata(type = EntityType.SCHEMA)
          String namespace,
      @Encoded() @PathParam("table") @AuthorizationMetadata(type = EntityType.TABLE) String table,
      PlanTableScanRequest scanRequest,
      @HeaderParam(X_ICEBERG_ACCESS_DELEGATION) String accessDelegation) {
    boolean isCredentialVending = isCredentialVending(accessDelegation);
    String catalogName = IcebergRESTUtils.getCatalogName(prefix);
    Namespace icebergNS = RESTUtil.decodeNamespace(namespace);
    String tableName = RESTUtil.decodeString(table);
    LOG.info(
        "Plan table scan, catalog: {}, namespace: {}, table: {}, "
            + "accessDelegation: {}, isCredentialVending: {}",
        catalogName,
        icebergNS,
        tableName,
        accessDelegation,
        isCredentialVending);

    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            TableIdentifier tableIdentifier = TableIdentifier.of(icebergNS, tableName);
            IcebergRequestContext context =
                new IcebergRequestContext(httpServletRequest(), catalogName, isCredentialVending);

            PlanTableScanResponse scanResponse =
                tableOperationDispatcher.planTableScan(context, tableIdentifier, scanRequest);

            if (isCredentialVending) {
              try {
                LoadCredentialsResponse credentialsResponse =
                    icebergCatalogWrapperManager
                        .getCatalogWrapper(catalogName)
                        .getCredentialsIfEligible(tableIdentifier, true, CredentialPrivilege.READ);
                if (!credentialsResponse.credentials().isEmpty()) {
                  return buildScanResponseWithCredentials(scanResponse, credentialsResponse);
                }
              } catch (Exception e) {
                LOG.warn(
                    "Failed to vend credentials for scan on table {}, "
                        + "returning scan response without credentials",
                    tableIdentifier,
                    e);
              }
            }

            return IcebergRESTUtils.ok(scanResponse);
          });
    } catch (Exception e) {
      LOG.error("Failed to plan table scan: {}", e.getMessage(), e);
      return IcebergExceptionMapper.toRESTResponse(e);
    }
  }

  /**
   * Filters the {@link LoadTableResponse} to include only snapshots that are directly referenced by
   * the table's refs (branches and tags). This implements the {@code snapshots=refs} query
   * parameter behavior per the Iceberg REST specification.
   *
   * @param loadTableResponse the original response containing all snapshots
   * @return a new response with only ref-referenced snapshots
   */
  @VisibleForTesting
  static LoadTableResponse filterSnapshotsByRefs(LoadTableResponse loadTableResponse) {
    TableMetadata metadata = loadTableResponse.tableMetadata();
    Map<String, SnapshotRef> refs = metadata.refs();
    Set<Long> referencedSnapshotIds =
        refs.values().stream().map(SnapshotRef::snapshotId).collect(Collectors.toSet());
    // If all snapshots are already referenced by refs, return the original response
    if (metadata.snapshots().stream()
        .allMatch(s -> referencedSnapshotIds.contains(s.snapshotId()))) {
      return loadTableResponse;
    }
    TableMetadata filteredMetadata =
        TableMetadata.buildFrom(metadata).suppressHistoricalSnapshots().build();
    return LoadTableResponse.builder()
        .withTableMetadata(filteredMetadata)
        .addAllConfig(loadTableResponse.config())
        .build();
  }

  private static Response buildResponseWithETag(LoadTableResponse loadTableResponse) {
    return IcebergRESTUtils.buildResponseWithETag(loadTableResponse);
  }

  private static Response buildResponseWithETag(
      LoadTableResponse loadTableResponse, Optional<EntityTag> etag) {
    return IcebergRESTUtils.buildResponseWithETag(loadTableResponse, etag);
  }

  @VisibleForTesting
  static Optional<EntityTag> generateETag(String metadataLocation) {
    return IcebergRESTUtils.generateETag(metadataLocation);
  }

  @VisibleForTesting
  static Optional<EntityTag> generateETag(String metadataLocation, String snapshots) {
    return IcebergRESTUtils.generateETag(metadataLocation, snapshots);
  }

  /**
   * Checks if the client's If-None-Match header value matches the current ETag.
   *
   * @param ifNoneMatch the If-None-Match header value from the client
   * @param etag the current ETag
   * @return true if the ETag matches (table unchanged), false otherwise
   */
  private static boolean etagMatches(String ifNoneMatch, EntityTag etag) {
    if (StringUtils.isBlank(ifNoneMatch)) {
      return false;
    }
    // Strip quotes if present to compare the raw value
    String clientEtag = ifNoneMatch.trim();
    if (clientEtag.startsWith("\"") && clientEtag.endsWith("\"")) {
      clientEtag = clientEtag.substring(1, clientEtag.length() - 1);
    }
    return etag.getValue().equals(clientEtag);
  }

  private boolean isCredentialVending(String accessDelegation) {
    if (StringUtils.isBlank(accessDelegation)) {
      return false;
    }
    if ("vended-credentials".equalsIgnoreCase(accessDelegation)) {
      return true;
    }
    if ("remote-signing".equalsIgnoreCase(accessDelegation)) {
      throw new UnsupportedOperationException(
          "Gravitino IcebergRESTServer doesn't support remote signing");
    } else {
      throw new IllegalArgumentException(
          X_ICEBERG_ACCESS_DELEGATION
              + ": "
              + accessDelegation
              + " is illegal, Iceberg REST spec supports: [vended-credentials,remote-signing], "
              + "Gravitino Iceberg REST server supports: vended-credentials");
    }
  }

  private Response buildScanResponseWithCredentials(
      PlanTableScanResponse scanResponse, LoadCredentialsResponse credentialsResponse) {
    ObjectNode responseNode = icebergObjectMapper.valueToTree(scanResponse);
    ArrayNode credArray = responseNode.putArray("storage-credentials");
    for (Credential cred : credentialsResponse.credentials()) {
      ObjectNode credNode = credArray.addObject();
      credNode.put("prefix", cred.prefix());
      ObjectNode configNode = credNode.putObject("config");
      cred.config().forEach(configNode::put);
    }
    return Response.ok(responseNode, MediaType.APPLICATION_JSON).build();
  }

  private NameIdentifier[] toNameIdentifiers(
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

  private ListTablesResponse filterListTablesResponse(
      ListTablesResponse listTablesResponse, String metalake, String catalogName) {
    NameIdentifier[] idents =
        MetadataAuthzHelper.filterByExpression(
            metalake,
            AuthorizationExpressionConstants.FILTER_TABLE_AUTHORIZATION_EXPRESSION,
            Entity.EntityType.TABLE,
            toNameIdentifiers(listTablesResponse, metalake, catalogName));
    List<TableIdentifier> filteredIdentifiers = new ArrayList<>();
    for (NameIdentifier ident : idents) {
      filteredIdentifiers.add(
          TableIdentifier.of(Namespace.of(ident.namespace().level(2)), ident.name()));
    }
    return ListTablesResponse.builder()
        .addAll(filteredIdentifiers)
        .nextPageToken(listTablesResponse.nextPageToken())
        .build();
  }
}
