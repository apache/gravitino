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

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.ws.rs.DELETE;
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
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager;
import org.apache.gravitino.paimon.service.PaimonMetadataHooks.Change;
import org.apache.gravitino.paimon.service.PaimonRESTUtils;
import org.apache.gravitino.server.authorization.expression.AuthorizationExpressionConstants;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.requests.AlterDatabaseRequest;
import org.apache.paimon.rest.requests.CreateDatabaseRequest;
import org.apache.paimon.rest.requests.RegisterTableRequest;
import org.apache.paimon.rest.responses.AlterDatabaseResponse;
import org.apache.paimon.rest.responses.GetDatabaseResponse;
import org.apache.paimon.rest.responses.ListDatabasesResponse;

/** Database endpoints of the Paimon REST protocol. */
@Path("/v1/{prefix:([^/]*/)?}databases")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PaimonDatabaseOperations {

  private static final String DATABASE_NAME_PATTERN = "databaseNamePattern";

  private final PaimonCatalogRequest catalogs;

  @Inject
  public PaimonDatabaseOperations(PaimonCatalogRequest catalogs) {
    this.catalogs = catalogs;
  }

  /**
   * Lists databases with pagination and an optional name pattern filter.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param maxResults the requested page size
   * @param pageToken the token of the previous page
   * @param pattern the SQL LIKE-style name pattern filter
   * @return the paged database names
   */
  @GET
  @Timed(name = "database.list." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "database.list", absolute = true)
  public Response listDatabases(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @QueryParam(PaimonRESTUtils.MAX_RESULTS) Integer maxResults,
      @QueryParam(PaimonRESTUtils.PAGE_TOKEN) String pageToken,
      @QueryParam(DATABASE_NAME_PATTERN) String pattern) {
    Map<String, String> query = queryParameterMap(maxResults, pageToken, pattern);
    Validation.validate(query);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));

    List<String> databases =
        PaimonRESTUtils.filterBySqlLikePattern(wrapper.listDatabases(), pattern).stream()
            .filter(database -> catalogs.canReadSchema(catalogName(rawPrefix), database))
            .collect(Collectors.toList());
    int pageSize = PaimonRESTUtils.getMaxResults(query);
    List<String> page =
        PaimonRESTUtils.buildPagedEntities(databases, name -> name, pageSize, pageToken);
    String nextPageToken = PaimonRESTUtils.nextPageToken(page, name -> name, pageSize);
    return PaimonRESTUtils.ok(new ListDatabasesResponse(page, nextPageToken));
  }

  /**
   * Creates a database.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param body the create database request
   * @return an empty 200 response
   */
  @POST
  @Timed(name = "database.create." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "database.create", absolute = true)
  public Response createDatabase(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix, String body) {
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    CreateDatabaseRequest request = PaimonRESTUtils.fromJson(body, CreateDatabaseRequest.class);
    catalogs.requireSchema(
        catalogName(rawPrefix),
        request.getName(),
        "ANY(OWNER, METALAKE, CATALOG, SCHEMA) || ANY_USE_CATALOG && ANY_CREATE_SCHEMA");
    catalogs.mutateSchema(
        Change.CREATE_SCHEMA,
        catalogName(rawPrefix),
        request.getName(),
        () -> {
          wrapper.createDatabase(request.getName(), request.getOptions());
          return null;
        });
    return Response.ok().build();
  }

  /**
   * Loads a database.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @return the database response
   */
  @GET
  @Path("{database}")
  @Timed(name = "database.load." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "database.load", absolute = true)
  public Response loadDatabase(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database) {
    database = RESTUtil.decodeString(database);
    PaimonCatalogWrapper wrapper = catalogs.getSchemaWrapper(catalogName(rawPrefix), database);
    Database metadata = wrapper.getDatabase(database);
    String id = UUID.nameUUIDFromBytes(database.getBytes(StandardCharsets.UTF_8)).toString();
    return PaimonRESTUtils.ok(
        new GetDatabaseResponse(
            id, metadata.name(), null, metadata.options(), null, 0L, null, 0L, null));
  }

  /**
   * Alters database properties.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param body the alter database request
   * @return the alter result
   */
  @POST
  @Path("{database}")
  @Timed(name = "database.alter." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "database.alter", absolute = true)
  public Response alterDatabase(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      String body) {
    database = RESTUtil.decodeString(database);
    catalogs.requireSchema(
        catalogName(rawPrefix),
        database,
        "ANY(OWNER, METALAKE, CATALOG) || SCHEMA_OWNER_WITH_USE_CATALOG");
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    AlterDatabaseRequest request = PaimonRESTUtils.fromJson(body, AlterDatabaseRequest.class);
    String databaseName = database;
    PaimonCatalogWrapper.AlterDatabaseResult result =
        catalogs.mutateSchema(
            Change.ALTER_SCHEMA,
            catalogName(rawPrefix),
            databaseName,
            () -> wrapper.alterDatabase(databaseName, request.getRemovals(), request.getUpdates()));
    return PaimonRESTUtils.ok(
        new AlterDatabaseResponse(result.removed(), result.updated(), result.missing()));
  }

  /**
   * Registers an existing table location as a table of the catalog (external table).
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param body the register table request
   * @return an empty 200 response
   */
  @POST
  @Path("{database}/register")
  @Timed(name = "table.register." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.register", absolute = true)
  public Response registerTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      String body) {
    database = RESTUtil.decodeString(database);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    RegisterTableRequest request = PaimonRESTUtils.fromJson(body, RegisterTableRequest.class);
    Identifier identifier = request.getIdentifier();
    if (identifier.getDatabaseName() == null) {
      identifier = Identifier.create(database, identifier.getObjectName());
    }
    if (!database.equals(identifier.getDatabaseName())) {
      throw new BadRequestException("Request database differs from the URL database.");
    }
    catalogs.requireSchema(
        catalogName(rawPrefix),
        database,
        AuthorizationExpressionConstants.CREATE_TABLE_AUTHORIZATION_EXPRESSION);
    Identifier target = identifier;
    catalogs.mutateTable(
        Change.CREATE_TABLE,
        catalogName(rawPrefix),
        target,
        () -> {
          wrapper.registerTable(target, request.getPath());
          return null;
        });
    return Response.ok().build();
  }

  /**
   * Drops an empty database.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @return an empty 200 response
   */
  @DELETE
  @Path("{database}")
  @Timed(name = "database.drop." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "database.drop", absolute = true)
  public Response dropDatabase(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database) {
    database = RESTUtil.decodeString(database);
    catalogs.requireSchema(
        catalogName(rawPrefix),
        database,
        "ANY(OWNER, METALAKE, CATALOG) || SCHEMA_OWNER_WITH_USE_CATALOG");
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    String databaseName = database;
    catalogs.mutateSchema(
        Change.DROP_SCHEMA,
        catalogName(rawPrefix),
        databaseName,
        () -> {
          wrapper.dropDatabase(databaseName);
          return null;
        });
    return Response.ok().build();
  }

  /**
   * Locates protocol operations that this server does not implement.
   *
   * @return the unsupported resource handler
   */
  @Path(
      "{database}/{resource:views|view-details|functions|function-details|table-details|policies|permissions}")
  public PaimonUnsupportedResource unsupportedResources() {
    return new PaimonUnsupportedResource();
  }

  static String catalogName(String rawPrefix) {
    if (rawPrefix == null || rawPrefix.isEmpty() || rawPrefix.equals("/")) {
      return PaimonCatalogWrapperManager.DEFAULT_CATALOG_NAME;
    }
    return RESTUtil.decodeString(
        rawPrefix.endsWith("/") ? rawPrefix.substring(0, rawPrefix.length() - 1) : rawPrefix);
  }

  static Map<String, String> queryParameterMap(
      Integer maxResults, String pageToken, String pattern) {
    Map<String, String> parameters = new java.util.HashMap<>();
    if (maxResults != null) {
      parameters.put(PaimonRESTUtils.MAX_RESULTS, String.valueOf(maxResults));
    }
    if (pageToken != null) {
      parameters.put(PaimonRESTUtils.PAGE_TOKEN, pageToken);
    }
    if (pattern != null) {
      parameters.put(DATABASE_NAME_PATTERN, pattern);
    }
    return parameters;
  }

  /** Resource type names and validation helpers reused by the table operations. */
  static final class Validation {
    private Validation() {}

    /**
     * Validates the common list query parameters, throwing a 400 error when invalid.
     *
     * @param parameters the query parameters
     */
    static void validate(Map<String, String> parameters) {
      if (!PaimonRESTUtils.isValidMaxResults(parameters)) {
        throw new BadRequestException(
            "Invalid input for queryParameter %s: %s",
            PaimonRESTUtils.MAX_RESULTS, parameters.get(PaimonRESTUtils.MAX_RESULTS));
      }
      for (Map.Entry<String, String> entry : parameters.entrySet()) {
        if (entry.getKey().endsWith("NamePattern")
            && !PaimonRESTUtils.isValidSqlLikePattern(entry.getValue())) {
          throw new BadRequestException(
              "Invalid input for queryParameter %s: %s", entry.getKey(), entry.getValue());
        }
      }
    }
  }
}
