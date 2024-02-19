/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.lakehouse.iceberg.web.rest;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.ops.IcebergTableOps;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergObjectMapper;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.IcebergRestUtils;
import com.datastrato.gravitino.catalog.lakehouse.iceberg.web.metrics.IcebergMetricsManager;
import com.datastrato.gravitino.metrics.MetricNames;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/v1/{prefix:([^/]*/)?}namespaces/{namespace}/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class IcebergTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableOperations.class);

  private IcebergTableOps icebergTableOps;
  private IcebergMetricsManager icebergMetricsManager;

  private ObjectMapper icebergObjectMapper;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public IcebergTableOperations(
      IcebergTableOps icebergTableOps, IcebergMetricsManager icebergMetricsManager) {
    this.icebergTableOps = icebergTableOps;
    this.icebergObjectMapper = IcebergObjectMapper.getInstance();
    this.icebergMetricsManager = icebergMetricsManager;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "list-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-table", absolute = true)
  public Response listTable(@PathParam("namespace") String namespace) {
    return IcebergRestUtils.ok(icebergTableOps.listTable(RESTUtil.decodeNamespace(namespace)));
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "create-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-table", absolute = true)
  public Response createTable(
      @PathParam("namespace") String namespace, CreateTableRequest createTableRequest) {
    LOG.info(
        "Create Iceberg table, namespace: {}, create table request: {}",
        namespace,
        createTableRequest);
    return IcebergRestUtils.ok(
        icebergTableOps.createTable(RESTUtil.decodeNamespace(namespace), createTableRequest));
  }

  @POST
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "update-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "update-table", absolute = true)
  public Response updateTable(
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      UpdateTableRequest updateTableRequest) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Update Iceberg table, namespace: {}, table: {}, updateTableRequest: {}",
          namespace,
          table,
          SerializeUpdateTableRequest(updateTableRequest));
    }
    TableIdentifier tableIdentifier =
        TableIdentifier.of(RESTUtil.decodeNamespace(namespace), table);
    return IcebergRestUtils.ok(icebergTableOps.updateTable(tableIdentifier, updateTableRequest));
  }

  @DELETE
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "drop-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-table", absolute = true)
  public Response dropTable(
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @DefaultValue("false") @QueryParam("purgeRequested") boolean purgeRequested) {
    LOG.info(
        "Drop Iceberg table, namespace: {}, table: {}, purgeRequested: {}",
        namespace,
        table,
        purgeRequested);
    TableIdentifier tableIdentifier =
        TableIdentifier.of(RESTUtil.decodeNamespace(namespace), table);
    if (purgeRequested) {
      icebergTableOps.purgeTable(tableIdentifier);
    } else {
      icebergTableOps.dropTable(tableIdentifier);
    }
    return IcebergRestUtils.noContent();
  }

  @GET
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "load-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-table", absolute = true)
  public Response loadTable(
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      @DefaultValue("all") @QueryParam("snapshots") String snapshots) {
    // todo support snapshots
    TableIdentifier tableIdentifier =
        TableIdentifier.of(RESTUtil.decodeNamespace(namespace), table);
    return IcebergRestUtils.ok(icebergTableOps.loadTable(tableIdentifier));
  }

  @HEAD
  @Path("{table}")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "table-exists." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table-exits", absolute = true)
  public Response tableExists(
      @PathParam("namespace") String namespace, @PathParam("table") String table) {
    TableIdentifier tableIdentifier =
        TableIdentifier.of(RESTUtil.decodeNamespace(namespace), table);
    if (icebergTableOps.tableExists(tableIdentifier)) {
      return IcebergRestUtils.okWithoutContent();
    } else {
      return IcebergRestUtils.notExists();
    }
  }

  @POST
  @Path("{table}/metrics")
  @Produces(MediaType.APPLICATION_JSON)
  @Timed(name = "report-table-metrics." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "report-table-metrics", absolute = true)
  public Response reportTableMetrics(
      @PathParam("namespace") String namespace,
      @PathParam("table") String table,
      ReportMetricsRequest request) {
    icebergMetricsManager.recordMetric(request.report());
    return IcebergRestUtils.noContent();
  }

  private String SerializeUpdateTableRequest(UpdateTableRequest updateTableRequest) {
    try {
      return icebergObjectMapper.writeValueAsString(updateTableRequest);
    } catch (JsonProcessingException e) {
      LOG.warn("Serialize update table request failed", e);
      return updateTableRequest.toString();
    }
  }
}
