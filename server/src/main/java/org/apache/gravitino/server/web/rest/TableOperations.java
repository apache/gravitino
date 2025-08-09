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

import static org.apache.gravitino.dto.util.DTOConverters.fromDTO;
import static org.apache.gravitino.dto.util.DTOConverters.fromDTOs;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
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
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.catalog.TableDispatcher;
import org.apache.gravitino.dto.requests.TableCreateRequest;
import org.apache.gravitino.dto.requests.TableUpdateRequest;
import org.apache.gravitino.dto.requests.TableUpdatesRequest;
import org.apache.gravitino.dto.responses.DropResponse;
import org.apache.gravitino.dto.responses.EntityListResponse;
import org.apache.gravitino.dto.responses.TableResponse;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.server.authorization.MetadataFilterHelper;
import org.apache.gravitino.server.authorization.annotations.AuthorizationExpression;
import org.apache.gravitino.server.authorization.annotations.AuthorizationMetadata;
import org.apache.gravitino.server.web.Utils;
import org.apache.gravitino.utils.NameIdentifierUtil;
import org.apache.gravitino.utils.NamespaceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables")
public class TableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(TableOperations.class);

  private static final String loadTableAuthorizationExpression =
      "ANY(OWNER, METALAKE, CATALOG) ||"
          + "SCHEMA_OWNER_WITH_USE_CATALOG ||"
          + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE || ANY_MODIFY_TABLE)";

  private final TableDispatcher dispatcher;

  @Context private HttpServletRequest httpRequest;

  @Inject
  public TableOperations(TableDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @GET
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "list-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "list-table", absolute = true)
  public Response listTables(
      @PathParam("metalake") String metalake,
      @PathParam("catalog") String catalog,
      @PathParam("schema") String schema) {
    LOG.info("Received list tables request for schema: {}.{}.{}", metalake, catalog, schema);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            Namespace tableNS = NamespaceUtil.ofTable(metalake, catalog, schema);
            NameIdentifier[] idents = dispatcher.listTables(tableNS);
            idents =
                MetadataFilterHelper.filterByExpression(
                    metalake, loadTableAuthorizationExpression, Entity.EntityType.TABLE, idents);
            Response response = Utils.ok(new EntityListResponse(idents));
            LOG.info(
                "List {} tables under schema: {}.{}.{}", idents.length, metalake, catalog, schema);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.LIST, "", schema, e);
    }
  }

  @POST
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "create-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "create-table", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA && ANY_CREATE_TABLE",
      accessMetadataType = MetadataObject.Type.SCHEMA)
  public Response createTable(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      TableCreateRequest request) {
    LOG.info(
        "Received create table request: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident =
                NameIdentifierUtil.ofTable(metalake, catalog, schema, request.getName());

            Table table =
                dispatcher.createTable(
                    ident,
                    fromDTOs(request.getColumns()),
                    request.getComment(),
                    request.getProperties(),
                    fromDTOs(request.getPartitioning()),
                    fromDTO(request.getDistribution()),
                    fromDTOs(request.getSortOrders()),
                    fromDTOs(request.getIndexes()));
            Response response = Utils.ok(new TableResponse(DTOConverters.toDTO(table)));
            LOG.info("Table created: {}.{}.{}.{}", metalake, catalog, schema, request.getName());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(
          OperationType.CREATE, request.getName(), schema, e);
    }
  }

  @GET
  @Path("{table}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "load-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "load-table", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_SELECT_TABLE|| ANY_MODIFY_TABLE)",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response loadTable(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("table") @AuthorizationMetadata(type = Entity.EntityType.TABLE) String table) {
    LOG.info(
        "Received load table request for table: {}.{}.{}.{}", metalake, catalog, schema, table);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofTable(metalake, catalog, schema, table);
            Table t = dispatcher.loadTable(ident);
            Response response = Utils.ok(new TableResponse(DTOConverters.toDTO(t)));
            LOG.info("Table loaded: {}.{}.{}.{}", metalake, catalog, schema, table);
            return response;
          });
    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.LOAD, table, schema, e);
    }
  }

  @PUT
  @Path("{table}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "alter-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "alter-table", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && (TABLE::OWNER || ANY_MODIFY_TABLE)",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response alterTable(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("table") @AuthorizationMetadata(type = Entity.EntityType.TABLE) String table,
      TableUpdatesRequest request) {
    LOG.info("Received alter table request: {}.{}.{}.{}", metalake, catalog, schema, table);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            request.validate();
            NameIdentifier ident = NameIdentifierUtil.ofTable(metalake, catalog, schema, table);
            TableChange[] changes =
                request.getUpdates().stream()
                    .map(TableUpdateRequest::tableChange)
                    .toArray(TableChange[]::new);
            Table t = dispatcher.alterTable(ident, changes);
            Response response = Utils.ok(new TableResponse(DTOConverters.toDTO(t)));
            LOG.info("Table altered: {}.{}.{}.{}", metalake, catalog, schema, t.name());
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.ALTER, table, schema, e);
    }
  }

  @DELETE
  @Path("{table}")
  @Produces("application/vnd.gravitino.v1+json")
  @Timed(name = "drop-table." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "drop-table", absolute = true)
  @AuthorizationExpression(
      expression =
          "ANY(OWNER, METALAKE, CATALOG) || "
              + "SCHEMA_OWNER_WITH_USE_CATALOG || "
              + "ANY_USE_CATALOG && ANY_USE_SCHEMA  && TABLE::OWNER ",
      accessMetadataType = MetadataObject.Type.TABLE)
  public Response dropTable(
      @PathParam("metalake") @AuthorizationMetadata(type = Entity.EntityType.METALAKE)
          String metalake,
      @PathParam("catalog") @AuthorizationMetadata(type = Entity.EntityType.CATALOG) String catalog,
      @PathParam("schema") @AuthorizationMetadata(type = Entity.EntityType.SCHEMA) String schema,
      @PathParam("table") @AuthorizationMetadata(type = Entity.EntityType.TABLE) String table,
      @QueryParam("purge") @DefaultValue("false") boolean purge) {
    LOG.info(
        "Received {} table request: {}.{}.{}.{}",
        purge ? "purge" : "drop",
        metalake,
        catalog,
        schema,
        table);
    try {
      return Utils.doAs(
          httpRequest,
          () -> {
            NameIdentifier ident = NameIdentifierUtil.ofTable(metalake, catalog, schema, table);
            boolean dropped = purge ? dispatcher.purgeTable(ident) : dispatcher.dropTable(ident);
            if (!dropped) {
              LOG.warn("Cannot find to be dropped table {} under schema {}", table, schema);
            }

            Response response = Utils.ok(new DropResponse(dropped));
            LOG.info(
                "Table {}: {}.{}.{}.{}",
                purge ? "purge" : "drop",
                metalake,
                catalog,
                schema,
                table);
            return response;
          });

    } catch (Exception e) {
      return ExceptionHandlers.handleTableException(OperationType.DROP, table, schema, e);
    }
  }
}
