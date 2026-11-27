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
package org.apache.gravitino.paimon.service.rest;

import static org.apache.gravitino.paimon.service.rest.PaimonDatabaseOperations.Validation;
import static org.apache.gravitino.paimon.service.rest.PaimonDatabaseOperations.catalogName;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.Encoded;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.gravitino.metrics.MetricNames;
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper;
import org.apache.gravitino.paimon.service.PaimonCatalogRequest;
import org.apache.gravitino.paimon.service.PaimonRESTUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.requests.RenameTableRequest;
import org.apache.paimon.rest.responses.ListTablesGloballyResponse;

/** Catalog-wide table endpoints of the Paimon REST protocol: global table listing and rename. */
@Path("/v1/{prefix:([^/]*/)?}tables")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PaimonTableRenameOperations {

  private final PaimonCatalogRequest catalogs;

  @Inject
  public PaimonTableRenameOperations(PaimonCatalogRequest catalogs) {
    this.catalogs = catalogs;
  }

  /**
   * Renames a table.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param body the rename table request
   * @return an empty 200 response
   */
  @POST
  @Path("rename")
  @Timed(name = "table.rename." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.rename", absolute = true)
  public Response renameTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix, String body) {
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    RenameTableRequest request = PaimonRESTUtils.fromJson(body, RenameTableRequest.class);
    wrapper.renameTable(request.getSource(), request.getDestination());
    return Response.ok().build();
  }

  /**
   * Lists tables across all databases of the catalog.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param maxResults the requested page size
   * @param pageToken the token of the previous page
   * @param databaseNamePattern the optional database name pattern
   * @param tableNamePattern the optional table name pattern
   * @return the paged table identifiers
   */
  @GET
  @Timed(name = "table.list-globally." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.list-globally", absolute = true)
  public Response listTablesGlobally(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @QueryParam(PaimonRESTUtils.MAX_RESULTS) Integer maxResults,
      @QueryParam(PaimonRESTUtils.PAGE_TOKEN) String pageToken,
      @QueryParam(RESTApi.DATABASE_NAME_PATTERN) String databaseNamePattern,
      @QueryParam(RESTApi.TABLE_NAME_PATTERN) String tableNamePattern) {
    Map<String, String> query =
        PaimonDatabaseOperations.queryParameterMap(maxResults, pageToken, databaseNamePattern);
    if (tableNamePattern != null) {
      query.put(RESTApi.TABLE_NAME_PATTERN, tableNamePattern);
    }
    Validation.validate(query);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    List<Identifier> allTables = new ArrayList<>();
    for (String databaseName :
        PaimonRESTUtils.filterBySqlLikePattern(wrapper.listDatabases(), databaseNamePattern)) {
      for (String tableName :
          PaimonRESTUtils.filterBySqlLikePattern(
              wrapper.listTables(databaseName), tableNamePattern)) {
        allTables.add(Identifier.create(databaseName, tableName));
      }
    }
    int pageSize = PaimonRESTUtils.getMaxResults(query);
    List<Identifier> page =
        PaimonRESTUtils.buildPagedEntities(
            allTables, PaimonTableRenameOperations::paginationKey, pageSize, pageToken);
    String nextPageToken =
        PaimonRESTUtils.nextPageToken(page, PaimonTableRenameOperations::paginationKey, pageSize);
    return PaimonRESTUtils.ok(new ListTablesGloballyResponse(page, nextPageToken));
  }

  /**
   * Loads a table by its REST uuid. Not supported in this version.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param tableId the table uuid
   * @return never; always throws
   */
  @GET
  @Path("id/{tableId}")
  @Timed(name = "table.load-by-id." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.load-by-id", absolute = true)
  public Response loadTableById(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("tableId") String tableId) {
    tableId = RESTUtil.decodeString(tableId);
    throw new NotImplementedException("Loading tables by id is not supported yet.");
  }

  private static String paginationKey(Identifier identifier) {
    // Escape dots in each component so the separator cannot collide with a name's dots.
    return RESTUtil.encodeString(identifier.getDatabaseName()).replace(".", "%2E")
        + "."
        + RESTUtil.encodeString(identifier.getObjectName()).replace(".", "%2E");
  }
}
