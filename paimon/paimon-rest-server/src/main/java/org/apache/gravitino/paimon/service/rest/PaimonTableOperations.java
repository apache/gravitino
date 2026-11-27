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
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.gravitino.paimon.common.PaimonCatalogWrapper.LoadedTable;
import org.apache.gravitino.paimon.service.PaimonCatalogRequest;
import org.apache.gravitino.paimon.service.PaimonCatalogWrapperManager.CatalogLease;
import org.apache.gravitino.paimon.service.PaimonRESTUtils;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.RESTUtil;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.NoSuchResourceException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.requests.AlterTableRequest;
import org.apache.paimon.rest.requests.CommitTableRequest;
import org.apache.paimon.rest.requests.CreateBranchRequest;
import org.apache.paimon.rest.requests.CreateTableRequest;
import org.apache.paimon.rest.requests.CreateTagRequest;
import org.apache.paimon.rest.requests.RollbackTableRequest;
import org.apache.paimon.rest.responses.AuditRESTResponse;
import org.apache.paimon.rest.responses.CommitTableResponse;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetTableResponse;
import org.apache.paimon.rest.responses.GetTableSnapshotResponse;
import org.apache.paimon.rest.responses.GetTableTokenResponse;
import org.apache.paimon.rest.responses.GetTagResponse;
import org.apache.paimon.rest.responses.GetVersionSnapshotResponse;
import org.apache.paimon.rest.responses.ListBranchesResponse;
import org.apache.paimon.rest.responses.ListSnapshotsResponse;
import org.apache.paimon.rest.responses.ListTablesResponse;
import org.apache.paimon.rest.responses.ListTagsResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Instant;
import org.apache.paimon.table.TableSnapshot;
import org.apache.paimon.tag.Tag;

/** Table, commit and snapshot endpoints of the Paimon REST protocol. */
@Path("/v1/{prefix:([^/]*/)?}databases/{database}/tables")
@Singleton
@Produces(MediaType.APPLICATION_JSON)
public class PaimonTableOperations {

  private static final List<String> STORAGE_OPTION_PREFIXES =
      List.of("s3.", "fs.s3a.", "fs.oss.", "fs.obs.", "fs.cos.", "fs.azure.", "fs.gs.", "fs.adl.");

  private static final String TABLE_NAME_PATTERN = "tableNamePattern";
  private static final String TAG_NAME_PREFIX = "tagNamePrefix";

  private final PaimonCatalogRequest catalogs;

  @Inject
  public PaimonTableOperations(PaimonCatalogRequest catalogs) {
    this.catalogs = catalogs;
  }

  /**
   * Lists table names of a database with pagination and an optional name pattern filter.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param maxResults the requested page size
   * @param pageToken the token of the previous page
   * @param pattern the SQL LIKE-style name pattern filter
   * @param tableType the optional Paimon table type
   * @return the paged table names
   */
  @GET
  @Timed(name = "table.list." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.list", absolute = true)
  public Response listTables(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @QueryParam(PaimonRESTUtils.MAX_RESULTS) Integer maxResults,
      @QueryParam(PaimonRESTUtils.PAGE_TOKEN) String pageToken,
      @QueryParam(TABLE_NAME_PATTERN) String pattern,
      @QueryParam(RESTApi.TABLE_TYPE) String tableType) {
    database = RESTUtil.decodeString(database);
    Map<String, String> query = queryParameterMap(maxResults, pageToken, pattern);
    Validation.validate(query);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    int pageSize = PaimonRESTUtils.getMaxResults(query);
    List<String> page =
        PaimonRESTUtils.buildPagedEntities(
            PaimonRESTUtils.filterBySqlLikePattern(
                wrapper.listTables(database, tableType), pattern),
            name -> name,
            pageSize,
            pageToken);
    String nextPageToken = PaimonRESTUtils.nextPageToken(page, name -> name, pageSize);
    return PaimonRESTUtils.ok(new ListTablesResponse(page, nextPageToken));
  }

  /**
   * Creates a table.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param body the create table request
   * @return an empty 200 response
   */
  @POST
  @Timed(name = "table.create." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.create", absolute = true)
  public Response createTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      String body) {
    database = RESTUtil.decodeString(database);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    CreateTableRequest request = PaimonRESTUtils.fromJson(body, CreateTableRequest.class);
    wrapper.createTable(request.getIdentifier(), request.getSchema());
    return Response.ok().build();
  }

  /**
   * Loads a table.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @return the table response
   */
  @GET
  @Path("{table}")
  @Timed(name = "table.load." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.load", absolute = true)
  public Response loadTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    Identifier identifier = identifier(database, table);
    LoadedTable loaded = wrapper.getTable(identifier);
    FileStoreTable nativeTable = loaded.table();
    Map<String, String> options = new HashMap<>(nativeTable.options());
    options.remove("path");
    options.remove(PaimonCatalogWrapper.TABLE_UUID_OPTION);
    // Paimon copies the response owner into the table properties that Flink requires to be
    // non-null.
    String owner = options.getOrDefault(AuditRESTResponse.FIELD_OWNER, "");
    Schema schema =
        new Schema(
            nativeTable.rowType().getFields(),
            nativeTable.partitionKeys(),
            nativeTable.primaryKeys(),
            options,
            nativeTable.comment().orElse(null));
    return PaimonRESTUtils.ok(
        new GetTableResponse(
            loaded.id(),
            identifier.getDatabaseName(),
            identifier.getObjectName(),
            nativeTable.location().toString(),
            false,
            nativeTable.schema().id(),
            schema,
            owner,
            0L,
            null,
            0L,
            null));
  }

  /**
   * Alters a table schema.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param body the alter table request
   * @return an empty 200 response
   */
  @POST
  @Path("{table}")
  @Timed(name = "table.alter." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.alter", absolute = true)
  public Response alterTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      String body) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    AlterTableRequest request = PaimonRESTUtils.fromJson(body, AlterTableRequest.class);
    wrapper.alterTable(identifier(database, table), request.getChanges());
    return Response.ok().build();
  }

  /**
   * Drops a table (Paimon drop equals purge).
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @return an empty 200 response
   */
  @DELETE
  @Path("{table}")
  @Timed(name = "table.drop." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.drop", absolute = true)
  public Response dropTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    wrapper.dropTable(identifier(database, table));
    return Response.ok().build();
  }

  /**
   * Commits a snapshot to a table. Returns {@code {"success":false}} (HTTP 200) when the snapshot
   * already exists so the client can regenerate and retry.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param body the commit table request
   * @return the commit result
   */
  @POST
  @Path("{table}/commit")
  @Timed(name = "table.commit." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.commit", absolute = true)
  public Response commitTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      String body) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    CommitTableRequest request = PaimonRESTUtils.fromJson(body, CommitTableRequest.class);
    boolean success =
        wrapper.commitTable(
            identifier(database, table),
            request.getTableId(),
            request.getBaseSnapshotUuid(),
            request.getSnapshot());
    return PaimonRESTUtils.ok(new CommitTableResponse(success));
  }

  /**
   * Loads the latest snapshot of a table.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @return the latest snapshot response
   */
  @GET
  @Path("{table}/snapshot")
  @Timed(name = "table.snapshot.load." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.snapshot.load", absolute = true)
  public Response loadTableSnapshot(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    Identifier identifier = identifier(database, table);
    TableSnapshot snapshot =
        wrapper
            .loadLatestTableSnapshot(identifier)
            .orElseThrow(
                () ->
                    new NoSuchResourceException(
                        ErrorResponse.RESOURCE_TYPE_SNAPSHOT,
                        null,
                        "Table %s does not have a snapshot yet.",
                        identifier.getFullName()));
    return PaimonRESTUtils.ok(new GetTableSnapshotResponse(snapshot));
  }

  /**
   * Loads a snapshot of a table by version: {@code EARLIEST}, {@code LATEST} or a numeric snapshot
   * id.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param version the version specifier
   * @return the snapshot response
   */
  @GET
  @Path("{table}/snapshots/{version}")
  @Timed(name = "table.snapshot.version." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.snapshot.version", absolute = true)
  public Response loadTableSnapshotByVersion(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @Encoded @PathParam("version") String version) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    version = RESTUtil.decodeString(version);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    Snapshot snapshot = wrapper.loadSnapshot(identifier(database, table), version);
    return PaimonRESTUtils.ok(new GetVersionSnapshotResponse(snapshot));
  }

  /**
   * Lists all snapshots of a table in ascending snapshot id order.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param maxResults the requested page size
   * @param pageToken the token of the previous page
   * @return the snapshot list response
   */
  @GET
  @Path("{table}/snapshots")
  @Timed(name = "table.snapshot.list." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.snapshot.list", absolute = true)
  public Response listTableSnapshots(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @QueryParam(PaimonRESTUtils.MAX_RESULTS) Integer maxResults,
      @QueryParam(PaimonRESTUtils.PAGE_TOKEN) String pageToken) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    List<Snapshot> snapshots = wrapper.listSnapshots(identifier(database, table));
    Map<String, String> query = queryParameterMap(maxResults, pageToken, null);
    Validation.validate(query);
    int pageSize = PaimonRESTUtils.getMaxResults(query);
    List<Snapshot> page =
        PaimonRESTUtils.buildPagedEntitiesNumeric(snapshots, Snapshot::id, pageSize, pageToken);
    String nextPageToken =
        PaimonRESTUtils.nextPageToken(page, snapshot -> String.valueOf(snapshot.id()), pageSize);
    return PaimonRESTUtils.ok(new ListSnapshotsResponse(page, nextPageToken));
  }

  // ------------------------------------------------------------------
  // Branches
  // ------------------------------------------------------------------

  /**
   * Lists the branches of a table.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @return the branch list response
   */
  @GET
  @Path("{table}/branches")
  @Timed(name = "table.branch.list." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.branch.list", absolute = true)
  public Response listBranches(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    return PaimonRESTUtils.ok(
        new ListBranchesResponse(wrapper.listBranches(identifier(database, table))));
  }

  /**
   * Creates a branch, optionally from a tag.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param body the create branch request
   * @return an empty 200 response
   */
  @POST
  @Path("{table}/branches")
  @Timed(name = "table.branch.create." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.branch.create", absolute = true)
  public Response createBranch(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      String body) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    CreateBranchRequest request = PaimonRESTUtils.fromJson(body, CreateBranchRequest.class);
    wrapper.createBranch(identifier(database, table), request.branch(), request.fromTag());
    return Response.ok().build();
  }

  /**
   * Drops a branch.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param branch the branch name
   * @return an empty 200 response
   */
  @DELETE
  @Path("{table}/branches/{branch}")
  @Timed(name = "table.branch.drop." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.branch.drop", absolute = true)
  public Response dropBranch(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @Encoded @PathParam("branch") String branch) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    branch = RESTUtil.decodeString(branch);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    wrapper.dropBranch(identifier(database, table), branch);
    return Response.ok().build();
  }

  /**
   * Renames a branch.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param branch the branch name
   * @param body the rename branch request
   * @return an empty 200 response
   */
  @POST
  @Path("{table}/branches/{branch}/rename")
  @Timed(name = "table.branch.rename." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.branch.rename", absolute = true)
  public Response renameBranch(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @Encoded @PathParam("branch") String branch,
      String body) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    branch = RESTUtil.decodeString(branch);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    // Paimon 1.x clients use toBranch. Paimon 2.1 no longer ships the request DTO.
    JsonNode request = PaimonRESTUtils.fromJson(body, JsonNode.class);
    String toBranch = request.path("toBranch").textValue();
    if (toBranch == null || toBranch.isEmpty()) {
      throw new BadRequestException("Missing branch rename destination.");
    }
    wrapper.renameBranch(identifier(database, table), branch, toBranch);
    return Response.ok().build();
  }

  /**
   * Fast-forwards a branch to the latest snapshot of the table.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param branch the branch name
   * @return an empty 200 response
   */
  @POST
  @Path("{table}/branches/{branch}/forward")
  @Timed(name = "table.branch.forward." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.branch.forward", absolute = true)
  public Response forwardBranch(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @Encoded @PathParam("branch") String branch) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    branch = RESTUtil.decodeString(branch);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    wrapper.fastForwardBranch(identifier(database, table), branch);
    return Response.ok().build();
  }

  // ------------------------------------------------------------------
  // Tags
  // ------------------------------------------------------------------

  /**
   * Lists the tags of a table with an optional name prefix filter.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param maxResults the requested page size
   * @param pageToken the token of the previous page
   * @param tagNamePrefix the tag name prefix filter
   * @return the paged tag names
   */
  @GET
  @Path("{table}/tags")
  @Timed(name = "table.tag.list." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.tag.list", absolute = true)
  public Response listTags(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @QueryParam(PaimonRESTUtils.MAX_RESULTS) Integer maxResults,
      @QueryParam(PaimonRESTUtils.PAGE_TOKEN) String pageToken,
      @QueryParam(TAG_NAME_PREFIX) String tagNamePrefix) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    Map<String, String> query = queryParameterMap(maxResults, pageToken, null);
    Validation.validate(query);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    int pageSize = PaimonRESTUtils.getMaxResults(query);
    List<String> tags = wrapper.listTags(identifier(database, table));
    if (tagNamePrefix != null && !tagNamePrefix.isEmpty()) {
      tags =
          tags.stream().filter(name -> name.startsWith(tagNamePrefix)).collect(Collectors.toList());
    }
    List<String> page = PaimonRESTUtils.buildPagedEntities(tags, name -> name, pageSize, pageToken);
    String nextPageToken = PaimonRESTUtils.nextPageToken(page, name -> name, pageSize);
    return PaimonRESTUtils.ok(new ListTagsResponse(page, nextPageToken));
  }

  /**
   * Loads a tag.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param tag the tag name
   * @return the tag response
   */
  @GET
  @Path("{table}/tags/{tag}")
  @Timed(name = "table.tag.load." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.tag.load", absolute = true)
  public Response loadTag(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @Encoded @PathParam("tag") String tag) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    tag = RESTUtil.decodeString(tag);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    Tag metadata = wrapper.getTag(identifier(database, table), tag);
    Long createdAt =
        metadata.getTagCreateTime() == null
            ? null
            : metadata.getTagCreateTime().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    String retention =
        metadata.getTagTimeRetained() == null ? null : metadata.getTagTimeRetained().toString();
    return PaimonRESTUtils.ok(
        new GetTagResponse(tag, metadata.trimToSnapshot(), createdAt, retention));
  }

  /**
   * Creates a tag on the latest snapshot or a given snapshot id.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param body the create tag request
   * @return an empty 200 response
   */
  @POST
  @Path("{table}/tags")
  @Timed(name = "table.tag.create." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.tag.create", absolute = true)
  public Response createTag(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      String body) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    CreateTagRequest request = PaimonRESTUtils.fromJson(body, CreateTagRequest.class);
    wrapper.createTag(
        identifier(database, table),
        request.tagName(),
        request.snapshotId(),
        request.timeRetained());
    return Response.ok().build();
  }

  /**
   * Deletes a tag.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param tag the tag name
   * @return an empty 200 response
   */
  @DELETE
  @Path("{table}/tags/{tag}")
  @Timed(name = "table.tag.drop." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.tag.drop", absolute = true)
  public Response dropTag(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      @Encoded @PathParam("tag") String tag) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    tag = RESTUtil.decodeString(tag);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    wrapper.deleteTag(identifier(database, table), tag);
    return Response.ok().build();
  }

  // ------------------------------------------------------------------
  // Rollback and unsupported sub-resources
  // ------------------------------------------------------------------

  /**
   * Rolls a table back to a snapshot or a tag.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @param body the rollback table request
   * @return an empty 200 response
   */
  @POST
  @Path("{table}/rollback")
  @Timed(name = "table.rollback." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.rollback", absolute = true)
  public Response rollbackTable(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table,
      String body) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    PaimonCatalogWrapper wrapper = catalogs.getCatalogWrapper(catalogName(rawPrefix));
    RollbackTableRequest request = PaimonRESTUtils.fromJson(body, RollbackTableRequest.class);
    Identifier identifier = identifier(database, table);
    Instant instant = request.getInstant();
    if (instant instanceof Instant.SnapshotInstant) {
      wrapper.rollbackTable(
          identifier,
          ((Instant.SnapshotInstant) instant).getSnapshotId(),
          request.getFromSnapshot());
    } else if (instant instanceof Instant.TagInstant) {
      wrapper.rollbackTableToTag(
          identifier, ((Instant.TagInstant) instant).getTagName(), request.getFromSnapshot());
    } else {
      throw new BadRequestException(
          "Unsupported rollback instant type: "
              + (instant == null ? "null" : instant.getClass().getSimpleName()));
    }
    return Response.ok().build();
  }

  /**
   * Rolls a table schema back. Not supported in this version.
   *
   * @return never; always throws
   */
  @POST
  @Path("{table}/rollback-schema")
  @Timed(name = "table.rollback-schema." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.rollback-schema", absolute = true)
  public Response rollbackSchema() {
    throw new NotImplementedException("Schema rollback is not supported yet.");
  }

  /**
   * Returns the table data token used by clients that enable {@code data-token.enabled}: the
   * storage credential options and their expiration time.
   *
   * @param rawPrefix the catalog prefix segment, may be empty
   * @param database the database name
   * @param table the table name
   * @return the token response
   */
  @GET
  @Path("{table}/token")
  @Timed(name = "table.token." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.token", absolute = true)
  public Response loadTableToken(
      @Encoded @PathParam("prefix") @DefaultValue("") String rawPrefix,
      @Encoded @PathParam("database") String database,
      @Encoded @PathParam("table") String table) {
    database = RESTUtil.decodeString(database);
    table = RESTUtil.decodeString(table);
    CatalogLease lease = catalogs.acquire(catalogName(rawPrefix));
    PaimonCatalogWrapper wrapper = lease.wrapper();
    Map<String, String> options = wrapper.catalogOptions().toMap();
    if (!Boolean.parseBoolean(options.get("data-token.enabled"))) {
      throw new NotImplementedException(
          "Table tokens are disabled for catalog %s; enable data-token.enabled to use them.",
          wrapper.name());
    }
    Map<String, String> storageOptions = new HashMap<>();
    options.forEach(
        (key, value) -> {
          if (STORAGE_OPTION_PREFIXES.stream().anyMatch(key::startsWith)) {
            storageOptions.put(key, value);
          }
        });
    long expiresAtMillis =
        System.currentTimeMillis() + 2 * RESTApi.TOKEN_EXPIRATION_SAFE_TIME_MILLIS;
    return PaimonRESTUtils.ok(new GetTableTokenResponse(storageOptions, expiresAtMillis));
  }

  /**
   * Lists partitions. Not supported in this version; Paimon clients transparently fall back to
   * filesystem partition listing when receiving 501.
   *
   * @return never; always throws
   */
  @GET
  @Path("{table}/partitions")
  @Timed(name = "table.partition.list." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.partition.list", absolute = true)
  public Response listPartitions() {
    throw new NotImplementedException(
        "Partitions are not served by the REST server; clients fall back to filesystem listing.");
  }

  /**
   * Rejects unsupported partition mutations and filtered partition listing.
   *
   * @return the unsupported resource handler
   */
  @Path("{table}/partitions/{operation:drop|list-by-filter}")
  public PaimonUnsupportedResource unsupportedPartitionOperations() {
    return new PaimonUnsupportedResource();
  }

  /**
   * Rejects unsupported partition registration.
   *
   * @return never; always throws
   */
  @POST
  @Path("{table}/partitions")
  public Response addPartitions() {
    throw new NotImplementedException("Partition registration is not supported yet.");
  }

  /**
   * Marks partitions done. Not supported in this version.
   *
   * @return never; always throws
   */
  @POST
  @Path("{table}/partitions/mark")
  @Timed(name = "table.partition.mark." + MetricNames.HTTP_PROCESS_DURATION, absolute = true)
  @ResponseMetered(name = "table.partition.mark", absolute = true)
  public Response markDonePartitions() {
    throw new NotImplementedException("Partition mark-done is not supported yet.");
  }

  /**
   * Lists partitions by names. Not supported in this version.
   *
   * @return never; always throws
   */
  @POST
  @Path("{table}/partitions/list-by-names")
  @Timed(
      name = "table.partition.list-by-names." + MetricNames.HTTP_PROCESS_DURATION,
      absolute = true)
  @ResponseMetered(name = "table.partition.list-by-names", absolute = true)
  public Response listPartitionsByNames() {
    throw new NotImplementedException("Partition listing by names is not supported yet.");
  }

  /**
   * Locates unsupported consumer and authorization operations.
   *
   * @return the unsupported resource handler
   */
  @Path("{table}/{resource:consumers|auth|policies|permissions|replace|schemas}")
  public PaimonUnsupportedResource unsupportedResources() {
    return new PaimonUnsupportedResource();
  }

  // ------------------------------------------------------------------
  // Private helpers
  // ------------------------------------------------------------------

  static Identifier identifier(String database, String table) {
    return Identifier.create(database, table);
  }

  private static Map<String, String> queryParameterMap(
      Integer maxResults, String pageToken, String pattern) {
    Map<String, String> parameters = new HashMap<>();
    if (maxResults != null) {
      parameters.put(PaimonRESTUtils.MAX_RESULTS, String.valueOf(maxResults));
    }
    if (pageToken != null) {
      parameters.put(PaimonRESTUtils.PAGE_TOKEN, pageToken);
    }
    if (pattern != null) {
      parameters.put(TABLE_NAME_PATTERN, pattern);
    }
    return parameters;
  }
}
