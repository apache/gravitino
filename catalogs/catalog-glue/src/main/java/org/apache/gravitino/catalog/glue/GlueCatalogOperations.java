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
package org.apache.gravitino.catalog.glue;

import static org.apache.gravitino.catalog.glue.GlueConstants.LOCATION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.catalog.hive.StorageFormat;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.NoSuchCatalogException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.NonEmptySchemaException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableCatalog;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.distributions.Strategy;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.PrincipalUtils;
import org.apache.iceberg.catalog.TableIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

/**
 * Operations implementation for the AWS Glue Data Catalog connector.
 *
 * <p>Implements schema CRUD (via {@link SupportsSchemas}) and table CRUD (via {@link TableCatalog})
 * backed by the AWS Glue API.
 */
public class GlueCatalogOperations implements CatalogOperations, SupportsSchemas, TableCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(GlueCatalogOperations.class);

  /**
   * Properties that identify the format of an Iceberg table, its metadata file and whether it was
   * registered; an alter cannot change them.
   */
  private static final Set<String> ICEBERG_RESERVED_KEYS =
      ImmutableSet.of(
          GlueConstants.TABLE_TYPE_PARAM,
          GlueConstants.TABLE_FORMAT,
          GlueConstants.METADATA_LOCATION,
          GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM);

  /** Table property keys that map to StorageDescriptor fields, not to Table.parameters(). */
  private static final Set<String> SD_TABLE_PROPERTY_KEYS =
      ImmutableSet.of(
          GlueConstants.LOCATION,
          GlueConstants.FORMAT,
          GlueConstants.INPUT_FORMAT_CLASS,
          GlueConstants.OUTPUT_FORMAT,
          GlueConstants.SERDE_LIB,
          GlueConstants.SERDE_NAME);

  /** Storage formats whose files can only be read by the SerDe of the same format. */
  private static final Set<StorageFormat> SERDE_BOUND_FORMATS =
      ImmutableSet.of(StorageFormat.ORC, StorageFormat.PARQUET, StorageFormat.AVRO);

  @VisibleForTesting GlueClient glueClient;

  /** Nullable — when null, Glue uses the caller's AWS account ID. */
  @VisibleForTesting String catalogId;

  /** Nullable — when null all table formats are exposed. */
  @VisibleForTesting Set<String> tableFormatFilter;

  @VisibleForTesting String defaultTableFormat;

  /**
   * Warehouse storage path. Table location is derived as {@code warehouse/db/table} for databases
   * that declare no {@code LocationUri}.
   */
  @VisibleForTesting String warehouseLocation;

  /** Iceberg SDK Glue catalog used for creating Iceberg-format tables. */
  @VisibleForTesting org.apache.iceberg.catalog.Catalog icebergGlueCatalog;

  private final GlueTypeConverter typeConverter = new GlueTypeConverter();

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.glueClient = GlueClientProvider.buildClient(config);
    String rawCatalogId = config.get(GlueConstants.AWS_GLUE_CATALOG_ID);
    this.catalogId = StringUtils.isNotBlank(rawCatalogId) ? rawCatalogId : null;
    this.defaultTableFormat =
        config.getOrDefault(
            GlueConstants.DEFAULT_TABLE_FORMAT, GlueConstants.DEFAULT_TABLE_FORMAT_VALUE);
    String filterProp =
        config.getOrDefault(
            GlueConstants.TABLE_FORMAT_FILTER, GlueConstants.DEFAULT_TABLE_FORMAT_FILTER);
    if (!GlueConstants.DEFAULT_TABLE_FORMAT_FILTER.equalsIgnoreCase(filterProp)) {
      tableFormatFilter =
          Arrays.stream(filterProp.split(","))
              .map(String::trim)
              .map(s -> s.toLowerCase(Locale.ROOT))
              .collect(Collectors.toSet());
    }
    this.warehouseLocation = config.get(GlueConstants.WAREHOUSE);
    this.icebergGlueCatalog = GlueIcebergTableHelper.createGlueCatalog(config);
  }

  @Override
  public void testConnection(NameIdentifier catalogIdent) {
    try {
      GetDatabasesRequest.Builder req = GetDatabasesRequest.builder().maxResults(1);
      applyCatalogId(catalogId, req::catalogId);
      glueClient.getDatabases(req.build());
    } catch (SdkException e) {
      throw GlueExceptionConverter.toConnectionException(e, glueClient);
    }
  }

  @Override
  public void close() throws IOException {
    if (glueClient != null) {
      glueClient.close();
      glueClient = null;
    }
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    List<NameIdentifier> result = new ArrayList<>();
    String nextToken = null;
    String context = "listing schemas under " + namespace;
    try {
      do {
        GetDatabasesRequest.Builder req = GetDatabasesRequest.builder();
        applyCatalogId(catalogId, req::catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetDatabasesResponse resp = callGlue(() -> glueClient.getDatabases(req.build()), context);
        resp.databaseList().stream()
            .map(db -> NameIdentifier.of(namespace, db.name()))
            .forEach(result::add);
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, context);
    }
    return result.toArray(new NameIdentifier[0]);
  }

  @Override
  public GlueSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {

    Map<String, String> params = properties != null ? new HashMap<>(properties) : new HashMap<>();
    // The location is stored only as the database LocationUri, which other engines read.
    String location = params.remove(LOCATION);

    DatabaseInput.Builder inputBuilder =
        DatabaseInput.builder().name(ident.name()).description(comment).parameters(params);
    if (StringUtils.isNotBlank(location)) {
      inputBuilder.locationUri(location);
    }

    DatabaseInput input = inputBuilder.build();

    CreateDatabaseRequest.Builder req = CreateDatabaseRequest.builder().databaseInput(input);
    applyCatalogId(catalogId, req::catalogId);

    try {
      callGlue(() -> glueClient.createDatabase(req.build()), "schema " + ident.name());
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + ident.name());
    }

    LOG.info("Created Glue schema (database) {}", ident.name());

    // The response carries the location like a later load, which reads it from LocationUri.
    Map<String, String> created = new HashMap<>(params);
    if (StringUtils.isNotBlank(location)) {
      created.put(LOCATION, location);
    }
    return GlueSchema.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(created)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentUserName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  @Override
  public GlueSchema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    GetDatabaseRequest.Builder req = GetDatabaseRequest.builder().name(ident.name());
    applyCatalogId(catalogId, req::catalogId);
    try {
      GlueSchema schema =
          GlueSchema.fromGlueDatabase(
              callGlue(() -> glueClient.getDatabase(req.build()), "schema " + ident.name())
                  .database());
      LOG.info("Loaded Glue schema (database) {}", ident.name());
      return schema;
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + ident.name());
    }
  }

  @Override
  public GlueSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    Database current = getDatabase(ident.name());

    // UpdateDatabase replaces the whole definition, so start from what Glue holds and change
    // only what the request asks for.
    Map<String, String> newParams = new HashMap<>();
    if (current.parameters() != null) {
      newParams.putAll(current.parameters());
    }
    String location = current.locationUri();
    if (StringUtils.isBlank(location)) {
      location = newParams.get(LOCATION);
    }
    newParams.remove(LOCATION);

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty sp = (SchemaChange.SetProperty) change;
        if (LOCATION.equals(sp.getProperty())) {
          location = sp.getValue();
        } else {
          newParams.put(sp.getProperty(), sp.getValue());
        }
      } else if (change instanceof SchemaChange.RemoveProperty) {
        String property = ((SchemaChange.RemoveProperty) change).getProperty();
        if (LOCATION.equals(property)) {
          location = null;
        } else {
          newParams.remove(property);
        }
      } else {
        throw new IllegalArgumentException(
            "Unsupported schema change: " + change.getClass().getSimpleName());
      }
    }

    DatabaseInput input =
        DatabaseInput.builder()
            .name(current.name())
            .description(current.description())
            .locationUri(StringUtils.isBlank(location) ? null : location)
            .parameters(newParams)
            .createTableDefaultPermissions(current.createTableDefaultPermissions())
            .targetDatabase(current.targetDatabase())
            .federatedDatabase(current.federatedDatabase())
            .build();

    UpdateDatabaseRequest.Builder req =
        UpdateDatabaseRequest.builder().name(ident.name()).databaseInput(input);
    applyCatalogId(catalogId, req::catalogId);

    try {
      callGlue(() -> glueClient.updateDatabase(req.build()), "schema " + ident.name());
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + ident.name());
    }

    LOG.info("Altered Glue schema (database) {}", ident.name());

    // The change is committed at this point, so a failed read falls back to the definition that
    // was sent rather than reporting the alter as failed.
    try {
      return loadSchema(ident);
    } catch (RuntimeException e) {
      LOG.warn(
          "Altered schema {}, but failed to read it back; returning the submitted definition",
          ident.name(),
          e);
      return GlueSchema.fromGlueDatabase(
          Database.builder()
              .name(input.name())
              .description(input.description())
              .locationUri(input.locationUri())
              .parameters(input.parameters())
              .createTime(current.createTime())
              .build());
    }
  }

  private Database getDatabase(String name) {
    GetDatabaseRequest.Builder req = GetDatabaseRequest.builder().name(name);
    applyCatalogId(catalogId, req::catalogId);
    try {
      return callGlue(() -> glueClient.getDatabase(req.build()), "schema " + name).database();
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + name);
    }
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (!cascade) {
      GetTablesRequest.Builder tabReq =
          GetTablesRequest.builder().databaseName(ident.name()).maxResults(1);
      applyCatalogId(catalogId, tabReq::catalogId);
      try {
        boolean hasTables =
            !callGlue(
                    () -> glueClient.getTables(tabReq.build()),
                    "checking tables in schema " + ident.name())
                .tableList()
                .isEmpty();
        if (hasTables) {
          throw new NonEmptySchemaException(
              "Schema %s is not empty. Use cascade=true to drop it with its tables.", ident.name());
        }
      } catch (EntityNotFoundException e) {
        return false;
      } catch (GlueException e) {
        throw GlueExceptionConverter.toSchemaException(
            e, "checking tables in schema " + ident.name());
      }
    }

    DeleteDatabaseRequest.Builder req = DeleteDatabaseRequest.builder().name(ident.name());
    applyCatalogId(catalogId, req::catalogId);
    try {
      callGlue(() -> glueClient.deleteDatabase(req.build()), "schema " + ident.name());
      LOG.info("Dropped Glue schema (database) {}", ident.name());
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + ident.name());
    }
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    String dbName = schemaName(namespace);
    List<NameIdentifier> result = new ArrayList<>();
    String nextToken = null;
    String context = "listing tables in schema " + dbName;
    try {
      do {
        GetTablesRequest.Builder req = GetTablesRequest.builder().databaseName(dbName);
        applyCatalogId(catalogId, req::catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetTablesResponse resp = callGlue(() -> glueClient.getTables(req.build()), context);
        resp.tableList().stream()
            .filter(t -> !isView(t))
            .filter(this::matchesFormatFilter)
            .map(t -> NameIdentifier.of(namespace, t.name()))
            .forEach(result::add);
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (EntityNotFoundException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", dbName);
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, context);
    }
    return result.toArray(new NameIdentifier[0]);
  }

  @Override
  public GlueTable loadTable(NameIdentifier ident) throws NoSuchTableException {
    String dbName = schemaName(ident.namespace());
    GetTableRequest.Builder req = GetTableRequest.builder().databaseName(dbName).name(ident.name());
    applyCatalogId(catalogId, req::catalogId);
    try {
      software.amazon.awssdk.services.glue.model.Table rawGlueTable =
          callGlue(() -> glueClient.getTable(req.build()), "table " + ident.name()).table();
      rejectIfView(rawGlueTable, ident, dbName);
      rejectIfFormatHidden(rawGlueTable, ident, dbName);
      GlueTable table = GlueTable.fromGlueTable(rawGlueTable, typeConverter);
      rejectDuplicateColumns(table, dbName);

      // The Iceberg metadata is the only source of the columns, partitioning and sort order of an
      // Iceberg table; the Glue columns are a Hive-style copy. A table whose metadata cannot be
      // read is reported as an error rather than with the Glue copy.
      if (GlueIcebergTableHelper.isIcebergTable(rawGlueTable)) {
        try {
          GlueIcebergTableHelper.loadTable(icebergGlueCatalog, dbName, ident.name(), table);
        } catch (RuntimeException e) {
          String metadataLocation = rawGlueTable.parameters().get(GlueConstants.METADATA_LOCATION);
          throw GlueExceptionConverter.toIcebergException(
              e,
              metadataLocation == null
                  ? "Iceberg metadata of table " + ident.name()
                  : String.format(
                      "Iceberg metadata %s of table %s", metadataLocation, ident.name()));
        }
      }

      table.initOpsContext(glueClient, catalogId, dbName);
      LOG.info("Loaded Glue table {}.{}", dbName, ident.name());
      return table;
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }
  }

  @Override
  public GlueTable createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders,
      Index[] indexes)
      throws NoSuchSchemaException, TableAlreadyExistsException {

    Preconditions.checkArgument(indexes.length == 0, "Glue catalog does not support indexes");

    String dbName = schemaName(ident.namespace());
    Map<String, String> props = properties != null ? properties : Collections.emptyMap();

    String tableFormat =
        props.getOrDefault(
            GlueConstants.TABLE_FORMAT,
            defaultTableFormat != null
                ? defaultTableFormat
                : GlueConstants.DEFAULT_TABLE_FORMAT_VALUE);
    Preconditions.checkArgument(
        GlueConstants.TABLE_FORMAT_HIVE.equalsIgnoreCase(tableFormat)
            || GlueConstants.TABLE_FORMAT_ICEBERG.equalsIgnoreCase(tableFormat),
        "Unsupported value '%s' of '%s': use %s or %s",
        tableFormat,
        GlueConstants.TABLE_FORMAT,
        GlueConstants.TABLE_FORMAT_HIVE,
        GlueConstants.TABLE_FORMAT_ICEBERG);
    boolean isIceberg = GlueConstants.TABLE_FORMAT_ICEBERG.equalsIgnoreCase(tableFormat);
    String normalizedFormat = tableFormat.toLowerCase(Locale.ROOT);
    // table-format only chooses how the table is created; the format of a stored table is its
    // table_type, so table-format is not stored.
    if (props.containsKey(GlueConstants.TABLE_FORMAT)) {
      props = new HashMap<>(props);
      props.remove(GlueConstants.TABLE_FORMAT);
    }
    // A table of an excluded format would be created but stay invisible through this catalog.
    Preconditions.checkArgument(
        isFormatVisible(normalizedFormat),
        "Cannot create table %s with format '%s': the catalog property '%s' only exposes %s",
        ident.name(),
        normalizedFormat,
        GlueConstants.TABLE_FORMAT_FILTER,
        tableFormatFilter);

    // Glue readers take table_type=ICEBERG as an Iceberg table, so a Hive-format table cannot carry
    // it.
    Preconditions.checkArgument(
        isIceberg
            || !GlueConstants.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
                props.get(GlueConstants.TABLE_TYPE_PARAM)),
        "Set '%s'=ICEBERG to create an Iceberg table; '%s'=ICEBERG is not allowed on a Hive-format"
            + " table",
        GlueConstants.TABLE_FORMAT,
        GlueConstants.TABLE_TYPE_PARAM);

    validateColumns(columns);
    // Iceberg tables delegate partitioning and ordering to GlueIcebergTableHelper.
    if (!isIceberg) {
      validateHivePartitioning(columns, partitions);
      validateHiveDistribution(distribution);
      validateHiveSortOrders(sortOrders);
    }

    if (isIceberg && props.containsKey(GlueConstants.METADATA_LOCATION)) {
      return registerIcebergTable(
          ident, dbName, comment, columns, props, partitions, distribution, sortOrders);
    }

    if (isIceberg) {
      Map<String, String> icebergProps = new HashMap<>(props);
      // Resolve location from warehouse if not explicitly provided.
      if (!icebergProps.containsKey(GlueConstants.LOCATION)) {
        icebergProps.put(GlueConstants.LOCATION, resolveTableLocation(null, dbName, ident.name()));
      }
      // Use Iceberg SDK to create the table (writes metadata.json to S3 and registers in Glue).
      try {
        GlueIcebergTableHelper.createTable(
            icebergGlueCatalog,
            dbName,
            ident.name(),
            columns,
            comment,
            icebergProps,
            partitions,
            sortOrders);
      } catch (RuntimeException e) {
        throw GlueExceptionConverter.toIcebergException(e, "table " + ident.name());
      }
      LOG.info("Created Iceberg table {}.{} via Iceberg SDK", dbName, ident.name());
      return loadTable(ident);
    }

    TableInput input =
        buildTableInput(
            dbName,
            ident.name(),
            comment,
            toLowerCaseColumns(columns),
            props,
            partitions,
            distribution,
            sortOrders);

    CreateTableRequest.Builder req =
        CreateTableRequest.builder().databaseName(dbName).tableInput(input);

    executeCreateTable(dbName, ident, req);
    LOG.info("Created Glue table {}.{}", dbName, ident.name());

    return loadAfterWrite(ident, dbName, input);
  }

  /**
   * Reads back what Glue stored, so the response carries the derived location and storage classes,
   * as a later load does. The write is committed at this point, so a failed read falls back to the
   * definition that was sent rather than reporting the operation as failed.
   */
  private GlueTable loadAfterWrite(NameIdentifier ident, String dbName, TableInput input) {
    try {
      return loadTable(ident);
    } catch (RuntimeException e) {
      LOG.warn(
          "Wrote table {}.{}, but failed to read it back; returning the submitted definition",
          dbName,
          ident.name(),
          e);
      GlueTable table = GlueTable.fromGlueTable(toTable(dbName, input), typeConverter);
      table.initOpsContext(glueClient, catalogId, dbName);
      return table;
    }
  }

  private static Table toTable(String dbName, TableInput input) {
    return Table.builder()
        .databaseName(dbName)
        .name(input.name())
        .description(input.description())
        .tableType(input.tableType())
        .parameters(input.parameters())
        .storageDescriptor(input.storageDescriptor())
        .partitionKeys(input.partitionKeys())
        .build();
  }

  @Override
  public GlueTable alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {

    String dbName = schemaName(ident.namespace());

    GetTableRequest.Builder rawReq =
        GetTableRequest.builder().databaseName(dbName).name(ident.name());
    applyCatalogId(catalogId, rawReq::catalogId);
    Table rawGlueTable;
    try {
      rawGlueTable =
          callGlue(() -> glueClient.getTable(rawReq.build()), "table " + ident.name()).table();
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }

    rejectIfView(rawGlueTable, ident, dbName);
    rejectIfFormatHidden(rawGlueTable, ident, dbName);

    if (GlueIcebergTableHelper.isIcebergTable(rawGlueTable)) {
      return alterIcebergTable(ident, dbName, rawGlueTable, changes);
    }

    return alterHiveTable(ident, dbName, rawGlueTable, changes);
  }

  /**
   * Alters a Hive-format table. Glue UpdateTable replaces the whole table definition, so the new
   * definition starts from the table as Glue holds it and only the requested parts change; fields
   * Gravitino does not model, and column types that are not changed, are written back as read. The
   * version read is sent along, so Glue refuses the update if the table changed in between.
   */
  private GlueTable alterHiveTable(
      NameIdentifier ident, String dbName, Table raw, TableChange... changes) {
    StorageDescriptor rawSd = raw.storageDescriptor();
    HiveColumns cols =
        new HiveColumns(
            rawSd != null && rawSd.hasColumns() ? rawSd.columns() : Collections.emptyList(),
            raw.hasPartitionKeys() ? raw.partitionKeys() : Collections.emptyList(),
            rawSd != null && rawSd.hasBucketColumns()
                ? rawSd.bucketColumns()
                : Collections.emptyList(),
            rawSd != null && rawSd.hasSortColumns() ? rawSd.sortColumns() : Collections.emptyList(),
            rawSd != null && rawSd.skewedInfo() != null && rawSd.skewedInfo().hasSkewedColumnNames()
                ? rawSd.skewedInfo().skewedColumnNames()
                : Collections.emptyList());
    String description = raw.description();
    Map<String, String> params =
        raw.hasParameters() ? new HashMap<>(raw.parameters()) : new HashMap<>();
    SerDeInfo rawSerde = rawSd != null ? rawSd.serdeInfo() : null;
    String location = rawSd != null ? rawSd.location() : null;
    String serdeName = rawSerde != null ? rawSerde.name() : null;
    Map<String, String> serdeParams =
        rawSerde != null && rawSerde.hasParameters()
            ? new HashMap<>(rawSerde.parameters())
            : new HashMap<>();
    boolean storageChanged = false;
    boolean changed = false;

    for (TableChange change : changes) {
      if (change instanceof TableChange.RenameTable) {
        throw new UnsupportedOperationException("Glue does not support table rename");
      } else if (change instanceof TableChange.UpdateComment) {
        description = ((TableChange.UpdateComment) change).getNewComment();
        changed = true;
      } else if (change instanceof TableChange.SetProperty) {
        // Storage properties map to storage descriptor fields, the rest to table parameters.
        TableChange.SetProperty sp = (TableChange.SetProperty) change;
        String property = sp.getProperty();
        // A table parameter of the same name would show again once the storage value is removed.
        if (isStorageProperty(property)) {
          params.remove(property);
        }
        if (GlueConstants.LOCATION.equals(property)) {
          location = sp.getValue();
          storageChanged = true;
        } else if (GlueConstants.SERDE_NAME.equals(property)) {
          serdeName = sp.getValue();
          storageChanged = true;
        } else if (property.startsWith(GlueConstants.SERDE_PARAMETER_PREFIX)) {
          serdeParams.put(
              property.substring(GlueConstants.SERDE_PARAMETER_PREFIX.length()), sp.getValue());
          storageChanged = true;
        } else {
          checkTableParameterKey(property);
          params.put(property, sp.getValue());
        }
        changed = true;
      } else if (change instanceof TableChange.RemoveProperty) {
        String property = ((TableChange.RemoveProperty) change).getProperty();
        // A table parameter of the same name is shown when the storage descriptor has no value, so
        // it is removed as well.
        if (isStorageProperty(property)) {
          changed |= params.remove(property) != null;
        }
        if (GlueConstants.LOCATION.equals(property)) {
          storageChanged |= location != null;
          location = null;
        } else if (GlueConstants.SERDE_NAME.equals(property)) {
          storageChanged |= serdeName != null;
          serdeName = null;
        } else if (property.startsWith(GlueConstants.SERDE_PARAMETER_PREFIX)) {
          storageChanged |=
              serdeParams.remove(property.substring(GlueConstants.SERDE_PARAMETER_PREFIX.length()))
                  != null;
        } else {
          checkTableParameterKey(property);
          changed |= params.remove(property) != null;
        }
        changed |= storageChanged;
      } else if (change instanceof TableChange.ColumnChange) {
        changed |= cols.apply((TableChange.ColumnChange) change);
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change: " + change.getClass().getSimpleName());
      }
    }

    if (!changed) {
      return loadTable(ident);
    }

    StorageDescriptor sd = rawSd;
    if (rawSd != null || !cols.dataCols.isEmpty() || storageChanged) {
      StorageDescriptor.Builder sdBuilder =
          rawSd != null ? rawSd.toBuilder() : StorageDescriptor.builder();
      sdBuilder.columns(cols.dataCols).bucketColumns(cols.bucketCols).sortColumns(cols.sortCols);
      if (rawSd != null && rawSd.skewedInfo() != null) {
        sdBuilder.skewedInfo(
            rawSd.skewedInfo().toBuilder().skewedColumnNames(cols.skewCols).build());
      }
      if (storageChanged) {
        SerDeInfo.Builder serdeBuilder =
            rawSerde != null ? rawSerde.toBuilder() : SerDeInfo.builder();
        sdBuilder
            .location(location)
            .serdeInfo(serdeBuilder.name(serdeName).parameters(serdeParams).build());
      }
      sd = sdBuilder.build();
    }

    TableInput input =
        tableInputFrom(raw)
            .description(description)
            .storageDescriptor(sd)
            .partitionKeys(cols.partCols)
            .parameters(params)
            .build();

    executeUpdateTable(
        ident,
        UpdateTableRequest.builder()
            .databaseName(dbName)
            .tableInput(input)
            .versionId(raw.versionId()));
    LOG.info("Altered Glue table {}.{}", dbName, ident.name());
    return loadAfterWrite(ident, dbName, input);
  }

  private static boolean isStorageProperty(String property) {
    return GlueConstants.LOCATION.equals(property)
        || GlueConstants.SERDE_NAME.equals(property)
        || property.startsWith(GlueConstants.SERDE_PARAMETER_PREFIX);
  }

  /**
   * The storage format and classes are set at create only, and so is the table format: changing it
   * would make a Hive-format table read as another format, or hide it behind the format filter.
   */
  private static void checkTableParameterKey(String property) {
    Preconditions.checkArgument(
        !SD_TABLE_PROPERTY_KEYS.contains(property)
            && !GlueConstants.TABLE_FORMAT.equals(property)
            && !GlueConstants.TABLE_TYPE_PARAM.equals(property),
        "Property %s of a Glue table cannot be changed",
        property);
  }

  /**
   * The column lists of a Hive-format table as Glue holds them, edited in place by column changes.
   * Glue column names are compared without regard to case, and new names are folded to lower case,
   * as Glue stores them.
   */
  private final class HiveColumns {
    private final List<software.amazon.awssdk.services.glue.model.Column> dataCols;
    private final List<software.amazon.awssdk.services.glue.model.Column> partCols;
    private final List<String> bucketCols;
    private final List<Order> sortCols;
    private final List<String> skewCols;

    private HiveColumns(
        List<software.amazon.awssdk.services.glue.model.Column> dataCols,
        List<software.amazon.awssdk.services.glue.model.Column> partCols,
        List<String> bucketCols,
        List<Order> sortCols,
        List<String> skewCols) {
      this.dataCols = new ArrayList<>(dataCols);
      this.partCols = new ArrayList<>(partCols);
      this.bucketCols = new ArrayList<>(bucketCols);
      this.sortCols = new ArrayList<>(sortCols);
      this.skewCols = new ArrayList<>(skewCols);
    }

    /**
     * Applies one column change.
     *
     * @return whether the table changed
     */
    private boolean apply(TableChange.ColumnChange change) {
      String[] fieldName = change.fieldName();
      Preconditions.checkArgument(
          fieldName != null && fieldName.length > 0, "Column change field name cannot be empty");
      Preconditions.checkArgument(
          fieldName.length == 1,
          "Glue Hive-format tables do not support changes to nested field %s; change the whole"
              + " column instead",
          String.join(".", fieldName));
      String name = fieldName[0];

      if (change instanceof TableChange.AddColumn) {
        TableChange.AddColumn add = (TableChange.AddColumn) change;
        Preconditions.checkArgument(
            !add.isAutoIncrement(), "Glue does not support auto-increment columns: %s", name);
        Preconditions.checkArgument(
            indexOf(dataCols, name) < 0 && indexOf(partCols, name) < 0,
            "Column %s already exists",
            name);
        software.amazon.awssdk.services.glue.model.Column column =
            software.amazon.awssdk.services.glue.model.Column.builder()
                .name(name.toLowerCase(Locale.ROOT))
                .type(toGlueType(name, add.getDataType()))
                .comment(add.getComment())
                .build();
        dataCols.add(insertIndex(add.getPosition(), dataCols.size()), column);
        return true;
      }

      if (change instanceof TableChange.UpdateColumnComment) {
        String comment = ((TableChange.UpdateColumnComment) change).getNewComment();
        List<software.amazon.awssdk.services.glue.model.Column> owner =
            indexOf(dataCols, name) >= 0 ? dataCols : partCols;
        int index = requireColumn(owner, name);
        owner.set(index, owner.get(index).toBuilder().comment(comment).build());
        return true;
      }

      Preconditions.checkArgument(
          indexOf(partCols, name) < 0,
          "Glue does not support %s on partition column %s",
          change.getClass().getSimpleName(),
          name);

      if (change instanceof TableChange.DeleteColumn) {
        int index = indexOf(dataCols, name);
        if (index < 0) {
          Boolean ifExists = ((TableChange.DeleteColumn) change).getIfExists();
          Preconditions.checkArgument(
              Boolean.TRUE.equals(ifExists), "Column %s does not exist", name);
          return false;
        }
        Preconditions.checkArgument(
            bucketCols.stream().noneMatch(name::equalsIgnoreCase)
                && sortCols.stream().noneMatch(o -> name.equalsIgnoreCase(o.column()))
                && skewCols.stream().noneMatch(name::equalsIgnoreCase),
            "Cannot delete column %s: it is a bucket, sort or skewed column of the table",
            name);
        dataCols.remove(index);
        return true;
      }

      int index = requireColumn(dataCols, name);
      software.amazon.awssdk.services.glue.model.Column old = dataCols.get(index);
      if (change instanceof TableChange.RenameColumn) {
        String newName = ((TableChange.RenameColumn) change).getNewName().toLowerCase(Locale.ROOT);
        Preconditions.checkArgument(
            (indexOf(dataCols, newName) < 0 || name.equalsIgnoreCase(newName))
                && indexOf(partCols, newName) < 0,
            "Column %s already exists",
            newName);
        dataCols.set(index, old.toBuilder().name(newName).build());
        bucketCols.replaceAll(c -> c.equalsIgnoreCase(name) ? newName : c);
        skewCols.replaceAll(c -> c.equalsIgnoreCase(name) ? newName : c);
        sortCols.replaceAll(
            o -> name.equalsIgnoreCase(o.column()) ? o.toBuilder().column(newName).build() : o);
      } else if (change instanceof TableChange.UpdateColumnType) {
        Type newType = ((TableChange.UpdateColumnType) change).getNewDataType();
        dataCols.set(index, old.toBuilder().type(toGlueType(name, newType)).build());
      } else if (change instanceof TableChange.UpdateColumnPosition) {
        dataCols.remove(index);
        dataCols.add(
            insertIndex(((TableChange.UpdateColumnPosition) change).getPosition(), dataCols.size()),
            old);
      } else {
        throw new IllegalArgumentException(
            "Unsupported column change: " + change.getClass().getSimpleName());
      }
      return true;
    }

    private int insertIndex(TableChange.ColumnPosition position, int defaultIndex) {
      if (position instanceof TableChange.First) {
        return 0;
      }
      if (position instanceof TableChange.After) {
        return requireColumn(dataCols, ((TableChange.After) position).getColumn()) + 1;
      }
      return defaultIndex;
    }

    private int requireColumn(
        List<software.amazon.awssdk.services.glue.model.Column> cols, String name) {
      int index = indexOf(cols, name);
      Preconditions.checkArgument(index >= 0, "Column %s does not exist", name);
      return index;
    }

    private int indexOf(List<software.amazon.awssdk.services.glue.model.Column> cols, String name) {
      for (int i = 0; i < cols.size(); i++) {
        if (cols.get(i).name().equalsIgnoreCase(name)) {
          return i;
        }
      }
      return -1;
    }
  }

  private GlueTable alterIcebergTable(
      NameIdentifier ident, String dbName, Table rawGlueTable, TableChange... changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        checkIcebergTableParameterKey(((TableChange.SetProperty) change).getProperty());
      } else if (change instanceof TableChange.RemoveProperty) {
        checkIcebergTableParameterKey(((TableChange.RemoveProperty) change).getProperty());
      }
    }
    if (isRegisteredIcebergTable(rawGlueTable)) {
      return alterRegisteredIcebergTable(ident, dbName, rawGlueTable, changes);
    }
    TableIdentifier finalId;
    try {
      finalId =
          GlueIcebergTableHelper.alterTable(icebergGlueCatalog, dbName, ident.name(), changes);
    } catch (RuntimeException e) {
      throw GlueExceptionConverter.toIcebergException(e, "altering table " + ident.name());
    }
    String newDbName = finalId.namespace().level(0);
    NameIdentifier finalIdent;
    if (newDbName.equals(dbName) && finalId.name().equals(ident.name())) {
      finalIdent = ident;
    } else {
      String[] levels = ident.namespace().levels().clone();
      levels[levels.length - 1] = newDbName;
      finalIdent = NameIdentifier.of(Namespace.of(levels), finalId.name());
    }
    return loadTable(finalIdent);
  }

  /**
   * Registers an existing Iceberg table from its metadata file. Iceberg reads the file before the
   * Glue table is created, and takes the Glue columns and location from it. The Glue table is then
   * marked as registered, so that later changes through Gravitino touch only its Glue parameters
   * and never write into the files it shares with the table it was registered from.
   */
  private GlueTable registerIcebergTable(
      NameIdentifier ident,
      String dbName,
      String comment,
      Column[] columns,
      Map<String, String> props,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders) {
    String metadataLocation = props.get(GlueConstants.METADATA_LOCATION);
    Preconditions.checkArgument(
        StringUtils.isNotBlank(metadataLocation),
        "Property %s cannot be blank",
        GlueConstants.METADATA_LOCATION);
    Preconditions.checkArgument(
        !props.containsKey(GlueConstants.LOCATION),
        "A registered Iceberg table takes its location from %s; do not set %s",
        GlueConstants.METADATA_LOCATION,
        GlueConstants.LOCATION);
    Preconditions.checkArgument(
        (partitions == null || partitions.length == 0)
            && (sortOrders == null || sortOrders.length == 0)
            && (distribution == null || distribution.strategy() == Strategy.NONE),
        "A registered Iceberg table takes its partitioning and sort order from %s; do not set"
            + " them",
        GlueConstants.METADATA_LOCATION);

    TableIdentifier tableId = TableIdentifier.of(dbName, ident.name());
    org.apache.iceberg.Table registered;
    try {
      registered = icebergGlueCatalog.registerTable(tableId, metadataLocation);
    } catch (RuntimeException e) {
      throw GlueExceptionConverter.toIcebergException(
          e, String.format("registering table %s from %s", ident.name(), metadataLocation));
    }

    // Iceberg writes only its own Glue parameters during registration, so the marker takes a second
    // update. A process stopped between the two leaves an unmarked table, which has to be marked by
    // hand.
    try {
      checkRegisteredColumns(columns, registered.schema(), metadataLocation);
      checkRegisteredProperties(props, registered.properties(), metadataLocation);
      Table raw = getRawTable(dbName, ident.name());
      Map<String, String> params = new HashMap<>(raw.parameters());
      props.forEach(params::putIfAbsent);
      params.put(GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM, "true");
      TableInput input =
          tableInputFrom(raw)
              .description(comment != null ? comment : raw.description())
              .parameters(params)
              .build();
      executeUpdateTable(
          ident,
          UpdateTableRequest.builder()
              .databaseName(dbName)
              .tableInput(input)
              .versionId(raw.versionId()));
    } catch (RuntimeException e) {
      // Without the marker the table would be altered as its own, so do not leave it behind.
      // Dropping the Glue entry keeps the metadata and data files.
      try {
        icebergGlueCatalog.dropTable(tableId, false);
      } catch (RuntimeException dropFailure) {
        e.addSuppressed(dropFailure);
      }
      throw e;
    }
    LOG.info("Registered Iceberg table {}.{} from {}", dbName, ident.name(), metadataLocation);
    return loadTable(ident);
  }

  /** Request columns of a registration are optional, and must match the metadata file if given. */
  private static void checkRegisteredColumns(
      Column[] columns, org.apache.iceberg.Schema schema, String metadataLocation) {
    if (columns == null || columns.length == 0) {
      return;
    }
    List<String> requested =
        Arrays.stream(columns)
            .map(c -> c.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.toList());
    List<String> stored =
        schema.columns().stream()
            .map(f -> f.name().toLowerCase(Locale.ROOT))
            .collect(Collectors.toList());
    Preconditions.checkArgument(
        requested.equals(stored),
        "Columns %s differ from the columns %s in %s; pass an empty column list to take them from"
            + " the metadata file",
        requested,
        stored,
        metadataLocation);
  }

  /**
   * A load reports the properties of the metadata file, so a request cannot give them new values.
   */
  private static void checkRegisteredProperties(
      Map<String, String> props, Map<String, String> metadataProps, String metadataLocation) {
    props.forEach(
        (key, value) ->
            Preconditions.checkArgument(
                !metadataProps.containsKey(key) || metadataProps.get(key).equals(value),
                "Property %s is set to '%s' in %s and cannot be given another value",
                key,
                metadataProps.get(key),
                metadataLocation));
  }

  /**
   * Alters an Iceberg table registered through Gravitino. Only its Glue parameters change; the
   * metadata file and the data files belong to the table it was registered from. Properties of the
   * metadata file cannot be changed: Iceberg engines read them from the file, and so does a load.
   */
  private GlueTable alterRegisteredIcebergTable(
      NameIdentifier ident, String dbName, Table rawGlueTable, TableChange... changes) {
    Map<String, String> newParams = new HashMap<>(rawGlueTable.parameters());
    Set<String> changedProperties = new HashSet<>();
    for (TableChange change : changes) {
      if (change instanceof TableChange.SetProperty) {
        TableChange.SetProperty sp = (TableChange.SetProperty) change;
        checkIcebergTableParameterKey(sp.getProperty());
        newParams.put(sp.getProperty(), sp.getValue());
        changedProperties.add(sp.getProperty());
      } else if (change instanceof TableChange.RemoveProperty) {
        String property = ((TableChange.RemoveProperty) change).getProperty();
        checkIcebergTableParameterKey(property);
        newParams.remove(property);
        changedProperties.add(property);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Iceberg table %s was registered from an existing metadata file, so only its"
                    + " properties can be changed; %s is not supported",
                ident.name(), change.getClass().getSimpleName()));
      }
    }

    Map<String, String> metadataProps;
    try {
      metadataProps =
          icebergGlueCatalog.loadTable(TableIdentifier.of(dbName, ident.name())).properties();
    } catch (RuntimeException e) {
      throw GlueExceptionConverter.toIcebergException(e, "altering table " + ident.name());
    }
    for (String property : changedProperties) {
      Preconditions.checkArgument(
          !metadataProps.containsKey(property),
          "Property %s of registered Iceberg table %s comes from its metadata file and cannot be"
              + " changed",
          property,
          ident.name());
    }

    TableInput input = tableInputFrom(rawGlueTable).parameters(newParams).build();
    executeUpdateTable(
        ident,
        UpdateTableRequest.builder()
            .databaseName(dbName)
            .tableInput(input)
            .versionId(rawGlueTable.versionId()));
    LOG.info("Altered registered Iceberg table {}.{} properties", dbName, ident.name());
    return loadTable(ident);
  }

  private static void checkIcebergTableParameterKey(String property) {
    Preconditions.checkArgument(
        !ICEBERG_RESERVED_KEYS.contains(property),
        "Property %s of an Iceberg table cannot be changed",
        property);
  }

  private static boolean isRegisteredIcebergTable(Table table) {
    return table.hasParameters()
        && "true"
            .equalsIgnoreCase(table.parameters().get(GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM));
  }

  /** Starts a Glue table definition from the table as Glue holds it, so no field is lost. */
  private static TableInput.Builder tableInputFrom(Table raw) {
    return TableInput.builder()
        .name(raw.name())
        .description(raw.description())
        .owner(raw.owner())
        .lastAccessTime(raw.lastAccessTime())
        .lastAnalyzedTime(raw.lastAnalyzedTime())
        .retention(raw.retention())
        .storageDescriptor(raw.storageDescriptor())
        .partitionKeys(raw.partitionKeys())
        .viewOriginalText(raw.viewOriginalText())
        .viewExpandedText(raw.viewExpandedText())
        .tableType(raw.tableType())
        .parameters(raw.parameters())
        .targetTable(raw.targetTable());
  }

  private Table getRawTable(String dbName, String tableName) {
    GetTableRequest.Builder req = GetTableRequest.builder().databaseName(dbName).name(tableName);
    applyCatalogId(catalogId, req::catalogId);
    try {
      return callGlue(() -> glueClient.getTable(req.build()), "table " + tableName).table();
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + tableName);
    }
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    String dbName = schemaName(ident.namespace());

    // Glue stores views as objects of type VIRTUAL_VIEW in the same namespace as tables, so the
    // object has to be fetched first to avoid dropping a view through the table API.
    GetTableRequest.Builder getReq =
        GetTableRequest.builder().databaseName(dbName).name(ident.name());
    applyCatalogId(catalogId, getReq::catalogId);
    try {
      Table existing =
          callGlue(() -> glueClient.getTable(getReq.build()), "table " + ident.name()).table();
      rejectIfView(existing, ident, dbName);
      if (!matchesFormatFilter(existing)) {
        return false;
      }
    } catch (EntityNotFoundException e) {
      return false;
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }

    DeleteTableRequest.Builder req =
        DeleteTableRequest.builder().databaseName(dbName).name(ident.name());
    applyCatalogId(catalogId, req::catalogId);
    try {
      callGlue(() -> glueClient.deleteTable(req.build()), "table " + ident.name());
      LOG.info("Dropped Glue table {}.{}", dbName, ident.name());
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }
  }

  /**
   * Drops an Iceberg table and deletes the data and metadata files its metadata references. Hive
   * tables in Glue are external, and registered Iceberg tables share their files with the table
   * they were registered from, so neither can be purged.
   */
  @Override
  public boolean purgeTable(NameIdentifier ident) throws UnsupportedOperationException {
    String dbName = schemaName(ident.namespace());
    Table existing;
    try {
      existing = getRawTable(dbName, ident.name());
    } catch (NoSuchTableException e) {
      return false;
    }
    rejectIfView(existing, ident, dbName);
    if (!matchesFormatFilter(existing)) {
      return false;
    }
    if (!GlueIcebergTableHelper.isIcebergTable(existing)) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot purge table %s: Glue Hive-format tables are external, so only drop is"
                  + " supported and the files stay in place",
              ident.name()));
    }
    if (isRegisteredIcebergTable(existing)) {
      throw new UnsupportedOperationException(
          String.format(
              "Cannot purge table %s: it was registered from an existing metadata file and shares"
                  + " its files with that table; drop it instead",
              ident.name()));
    }
    try {
      boolean purged = icebergGlueCatalog.dropTable(TableIdentifier.of(dbName, ident.name()), true);
      LOG.info("Purged Iceberg table {}.{}", dbName, ident.name());
      return purged;
    } catch (RuntimeException e) {
      throw GlueExceptionConverter.toIcebergException(e, "purging table " + ident.name());
    }
  }

  private static String schemaName(Namespace namespace) {
    String[] levels = namespace.levels();
    Preconditions.checkArgument(
        levels.length >= 2, "Namespace must have at least 2 levels, got: %s", levels.length);
    return levels[levels.length - 1];
  }

  /**
   * Returns whether the Glue object is a view rather than a table. Glue keeps views and tables in
   * the same namespace and returns both from the table APIs, so every table entry point has to
   * screen them out.
   */
  private static boolean isView(Table table) {
    return GlueConstants.VIRTUAL_VIEW_TABLE_TYPE.equalsIgnoreCase(table.tableType());
  }

  /**
   * Glue accepts a table whose columns share a name (Glue folds names to lower case), but such a
   * table cannot be represented in Gravitino.
   */
  private static void rejectDuplicateColumns(GlueTable table, String dbName) {
    Set<String> names = new HashSet<>();
    for (Column column : table.columns()) {
      Preconditions.checkArgument(
          names.add(column.name().toLowerCase(Locale.ROOT)),
          "Table %s.%s has more than one column named %s; fix the table in Glue",
          dbName,
          table.name(),
          column.name());
    }
  }

  /** Returns the columns under the lower-case names Glue stores for a Hive-format table. */
  private static Column[] toLowerCaseColumns(Column[] columns) {
    return Arrays.stream(columns)
        .map(
            column ->
                Column.of(
                    column.name().toLowerCase(Locale.ROOT),
                    column.dataType(),
                    column.comment(),
                    column.nullable(),
                    column.autoIncrement(),
                    column.defaultValue()))
        .toArray(Column[]::new);
  }

  /** Checks the column options Glue cannot store and the names Glue would fold together. */
  private static void validateColumns(Column[] columns) {
    Set<String> names = new HashSet<>();
    for (Column column : columns) {
      Preconditions.checkArgument(
          !column.autoIncrement(),
          "Glue does not support auto-increment columns: %s",
          column.name());
      // Glue stores column names in lower case.
      Preconditions.checkArgument(
          names.add(column.name().toLowerCase(Locale.ROOT)),
          "Duplicate column name: %s; Glue column names are case-insensitive",
          column.name());
    }
  }

  /**
   * Checks that a Hive-format table is partitioned by identity transforms on distinct primitive
   * columns that are the last columns of the table, in partitioning order, because Glue stores the
   * partition columns as a separate trailing list.
   */
  private static void validateHivePartitioning(Column[] columns, Transform[] partitions) {
    Preconditions.checkArgument(
        columns.length >= partitions.length,
        "The table has %s columns but %s partition columns",
        columns.length,
        partitions.length);
    int firstPartitionColumn = columns.length - partitions.length;
    for (int i = 0; i < partitions.length; i++) {
      Transform t = partitions[i];
      Preconditions.checkArgument(
          t instanceof Transforms.IdentityTransform,
          "Glue catalog only supports identity partitioning, got: %s",
          t.name());
      String[] fieldName = ((Transforms.IdentityTransform) t).fieldName();
      Preconditions.checkArgument(
          fieldName.length == 1, "Glue catalog does not support nested field partitioning");
      Column column = columns[firstPartitionColumn + i];
      Preconditions.checkArgument(
          column.name().equalsIgnoreCase(fieldName[0]),
          "Partition column %s must be at position %s: partition columns must be the last columns"
              + " of the table, in partitioning order",
          fieldName[0],
          firstPartitionColumn + i + 1);
      Type type = column.dataType();
      Preconditions.checkArgument(
          !(type instanceof Types.StructType
              || type instanceof Types.ListType
              || type instanceof Types.MapType
              || type instanceof Types.UnionType),
          "Partition column %s has type %s; Glue partition columns must have a primitive type",
          column.name(),
          type.simpleString());
    }
  }

  /** Checks that a distribution is one Glue can store: hash bucketing on plain columns. */
  private static void validateHiveDistribution(Distribution distribution) {
    if (distribution == null || distribution.strategy() == Strategy.NONE) {
      return;
    }
    Preconditions.checkArgument(
        distribution.strategy() == Strategy.HASH,
        "Glue only supports hash distribution (bucketing), got: %s",
        distribution.strategy());
    Preconditions.checkArgument(
        distribution.number() >= 1,
        "Glue bucketing needs a number of buckets of at least 1, got: %s",
        distribution.number());
    Preconditions.checkArgument(
        distribution.expressions().length > 0, "Glue bucketing needs at least one bucket column");
    for (Expression e : distribution.expressions()) {
      Preconditions.checkArgument(
          e instanceof NamedReference.FieldReference
              && ((NamedReference.FieldReference) e).fieldName().length == 1,
          "Glue only supports bucketing on plain columns, got: %s",
          e);
    }
  }

  /** Checks that sort orders are ones Glue can store: plain columns with the default nulls. */
  private static void validateHiveSortOrders(SortOrder[] sortOrders) {
    if (sortOrders == null) {
      return;
    }
    for (SortOrder so : sortOrders) {
      Preconditions.checkArgument(
          so.expression() instanceof NamedReference.FieldReference
              && ((NamedReference.FieldReference) so.expression()).fieldName().length == 1,
          "Glue only supports sort orders on plain columns, got: %s",
          so.expression());
      // Glue stores only the sort direction; the null ordering is implied by it.
      Preconditions.checkArgument(
          so.nullOrdering() == so.direction().defaultNullOrdering(),
          "Glue does not support %s for a %s sort order on %s",
          so.nullOrdering(),
          so.direction(),
          so.expression());
    }
  }

  /** Rejects a Glue object that is a view, so that it is not acted on through the table API. */
  private static void rejectIfView(Table table, NameIdentifier ident, String dbName) {
    if (isView(table)) {
      throw new NoSuchTableException(
          "No table named %s in schema %s (it is a view, not a table)", ident.name(), dbName);
    }
  }

  // NOTE: parameter type is the Glue SDK Table, not GlueTable (our domain class).
  // The Glue SDK's Column model is also referenced by FQN throughout this class because its
  // simple name conflicts with the imported org.apache.gravitino.rel.Column.
  /**
   * A table is an Iceberg table when its native {@code table_type} parameter says so, and a
   * Hive-format table otherwise, the same classification every other operation uses.
   */
  private boolean matchesFormatFilter(Table table) {
    if (tableFormatFilter == null) return true;
    String format =
        GlueIcebergTableHelper.isIcebergTable(table)
            ? GlueConstants.TABLE_FORMAT_ICEBERG
            : GlueConstants.TABLE_FORMAT_HIVE;
    return isFormatVisible(format.toLowerCase(Locale.ROOT));
  }

  private boolean isFormatVisible(String normalizedFormat) {
    return tableFormatFilter == null
        || tableFormatFilter.contains(GlueConstants.DEFAULT_TABLE_FORMAT_FILTER)
        || tableFormatFilter.contains(normalizedFormat);
  }

  /**
   * Throws {@link NoSuchTableException} for a table whose format is excluded by {@code
   * table-format-filter}, so that load and alter treat it as absent.
   */
  private void rejectIfFormatHidden(Table table, NameIdentifier ident, String dbName) {
    if (!matchesFormatFilter(table)) {
      throw new NoSuchTableException(
          "Table %s.%s does not exist: its format is excluded by catalog property '%s'",
          dbName, ident.name(), GlueConstants.TABLE_FORMAT_FILTER);
    }
  }

  private void executeCreateTable(
      String dbName, NameIdentifier ident, CreateTableRequest.Builder req) {
    applyCatalogId(catalogId, req::catalogId);
    try {
      callGlue(() -> glueClient.createTable(req.build()), "table " + ident.name());
    } catch (EntityNotFoundException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", dbName);
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }
  }

  private void executeUpdateTable(NameIdentifier ident, UpdateTableRequest.Builder req) {
    applyCatalogId(catalogId, req::catalogId);
    try {
      callGlue(() -> glueClient.updateTable(req.build()), "table " + ident.name());
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }
  }

  private TableInput buildTableInput(
      String dbName,
      String name,
      String comment,
      Column[] columns,
      Map<String, String> properties,
      Transform[] partitions,
      Distribution distribution,
      SortOrder[] sortOrders) {

    int partCount = partitions.length;
    Preconditions.checkArgument(
        columns.length >= partCount,
        "columns.length (%s) must be >= number of partition columns (%s)",
        columns.length,
        partCount);
    int dataCount = columns.length - partCount;

    List<software.amazon.awssdk.services.glue.model.Column> glueDataCols = new ArrayList<>();
    for (int i = 0; i < dataCount; i++) {
      glueDataCols.add(toGlueColumn(columns[i]));
    }

    List<software.amazon.awssdk.services.glue.model.Column> gluePartCols = new ArrayList<>();
    for (int i = dataCount; i < columns.length; i++) {
      gluePartCols.add(toGlueColumn(columns[i]));
    }

    // Separate properties into: SD fields, table-level fields, and Table.parameters()
    Map<String, String> serdeParams = new HashMap<>();
    Map<String, String> tableParams = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith(GlueConstants.SERDE_PARAMETER_PREFIX)) {
        serdeParams.put(
            key.substring(GlueConstants.SERDE_PARAMETER_PREFIX.length()), entry.getValue());
      } else if (!SD_TABLE_PROPERTY_KEYS.contains(key)) {
        tableParams.put(key, entry.getValue());
      }
    }

    // Blank classes cannot name a storage format, so they are treated as not set.
    String inputFormat = StringUtils.trimToNull(properties.get(GlueConstants.INPUT_FORMAT_CLASS));
    String outputFormat = StringUtils.trimToNull(properties.get(GlueConstants.OUTPUT_FORMAT));
    String serdeLib = StringUtils.trimToNull(properties.get(GlueConstants.SERDE_LIB));
    StorageFormat sf =
        resolveStorageFormat(
            StringUtils.trimToNull(properties.get(GlueConstants.FORMAT)),
            inputFormat,
            outputFormat,
            serdeLib);
    if (sf != null) {
      inputFormat = sf.getInputFormat();
      outputFormat = sf.getOutputFormat();
      serdeLib = sf.getSerde();
    }

    SerDeInfo serDe =
        SerDeInfo.builder()
            .serializationLibrary(serdeLib)
            .name(properties.get(GlueConstants.SERDE_NAME))
            .parameters(serdeParams)
            .build();

    List<String> bucketCols = Collections.emptyList();
    int numBuckets = 0;
    if (distribution != null && distribution != Distributions.NONE) {
      numBuckets = distribution.number();
      bucketCols =
          Arrays.stream(distribution.expressions())
              .filter(e -> e instanceof NamedReference.FieldReference)
              .map(e -> String.join(".", ((NamedReference.FieldReference) e).fieldName()))
              .map(column -> column.toLowerCase(Locale.ROOT))
              .collect(Collectors.toList());
    }

    List<Order> glueSortCols = new ArrayList<>();
    if (sortOrders != null) {
      for (SortOrder so : sortOrders) {
        if (so.expression() instanceof NamedReference.FieldReference) {
          String colName =
              String.join(".", ((NamedReference.FieldReference) so.expression()).fieldName())
                  .toLowerCase(Locale.ROOT);
          int order = so.direction() == SortDirection.ASCENDING ? 1 : 0;
          glueSortCols.add(Order.builder().column(colName).sortOrder(order).build());
        }
      }
    }

    StorageDescriptor sd =
        StorageDescriptor.builder()
            .columns(glueDataCols)
            .location(resolveTableLocation(properties.get(GlueConstants.LOCATION), dbName, name))
            .inputFormat(inputFormat)
            .outputFormat(outputFormat)
            .serdeInfo(serDe)
            .bucketColumns(bucketCols)
            .numberOfBuckets(numBuckets)
            .sortColumns(glueSortCols)
            .build();

    return TableInput.builder()
        .name(name)
        .description(comment)
        .tableType(GlueConstants.EXTERNAL_TABLE_TYPE)
        .parameters(tableParams)
        .storageDescriptor(sd)
        .partitionKeys(gluePartCols)
        .build();
  }

  private software.amazon.awssdk.services.glue.model.Column toGlueColumn(Column col) {
    return software.amazon.awssdk.services.glue.model.Column.builder()
        .name(col.name())
        .type(toGlueType(col.name(), col.dataType()))
        .comment(col.comment())
        .build();
  }

  private String toGlueType(String columnName, Type type) {
    try {
      return typeConverter.fromGravitino(type);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Column %s: %s", columnName, e.getMessage()), e);
    }
  }

  /**
   * Resolves the storage classes of a Hive-format table. A {@code format} sets all three classes,
   * and any class given next to it must match. Without a format, either all three classes are
   * given, or none and the table is a text table, as in Hive and Athena.
   *
   * @return the storage format to apply, or null when all three classes are given explicitly
   */
  private static StorageFormat resolveStorageFormat(
      String format, String inputFormat, String outputFormat, String serdeLib) {
    if (format == null) {
      // Built-in input and output classes must come from the same storage format; a custom SerDe
      // over a built-in format, such as a JSON SerDe over text files, is allowed.
      Preconditions.checkArgument(
          !isBuiltInInputFormat(inputFormat)
              || !isBuiltInOutputFormat(outputFormat)
              || Arrays.stream(StorageFormat.values())
                  .anyMatch(
                      f ->
                          f.getInputFormat().equals(inputFormat)
                              && f.getOutputFormat().equals(outputFormat)),
          "'%s' %s and '%s' %s belong to different storage formats",
          GlueConstants.INPUT_FORMAT_CLASS,
          inputFormat,
          GlueConstants.OUTPUT_FORMAT,
          outputFormat);
      boolean anyClass = inputFormat != null || outputFormat != null || serdeLib != null;
      if (!anyClass) {
        return StorageFormat.TEXTFILE;
      }
      Preconditions.checkArgument(
          inputFormat != null && outputFormat != null && serdeLib != null,
          "Set all of '%s', '%s' and '%s', or set '%s' instead",
          GlueConstants.INPUT_FORMAT_CLASS,
          GlueConstants.OUTPUT_FORMAT,
          GlueConstants.SERDE_LIB,
          GlueConstants.FORMAT);
      // A custom class is taken as given, but these formats are read only by their own SerDe.
      Preconditions.checkArgument(
          !isBuiltInInputFormat(inputFormat)
              || !isBuiltInSerde(serdeLib)
              || SERDE_BOUND_FORMATS.stream()
                  .allMatch(
                      f -> f.getInputFormat().equals(inputFormat) == f.getSerde().equals(serdeLib)),
          "'%s' %s and '%s' %s belong to different storage formats",
          GlueConstants.INPUT_FORMAT_CLASS,
          inputFormat,
          GlueConstants.SERDE_LIB,
          serdeLib);
      return null;
    }

    String upperFormat = format.trim().toUpperCase(Locale.ROOT);
    Preconditions.checkArgument(
        Arrays.stream(StorageFormat.values()).anyMatch(f -> f.name().equals(upperFormat)),
        "Unsupported table format: %s. Supported formats: %s",
        format,
        Arrays.toString(StorageFormat.values()));
    StorageFormat sf = StorageFormat.valueOf(upperFormat);
    checkFormatClass(format, GlueConstants.INPUT_FORMAT_CLASS, inputFormat, sf.getInputFormat());
    checkFormatClass(format, GlueConstants.OUTPUT_FORMAT, outputFormat, sf.getOutputFormat());
    checkFormatClass(format, GlueConstants.SERDE_LIB, serdeLib, sf.getSerde());
    return sf;
  }

  private static boolean isBuiltInInputFormat(String inputFormat) {
    return Arrays.stream(StorageFormat.values())
        .anyMatch(f -> f.getInputFormat().equals(inputFormat));
  }

  private static boolean isBuiltInOutputFormat(String outputFormat) {
    return Arrays.stream(StorageFormat.values())
        .anyMatch(f -> f.getOutputFormat().equals(outputFormat));
  }

  private static boolean isBuiltInSerde(String serdeLib) {
    return Arrays.stream(StorageFormat.values()).anyMatch(f -> f.getSerde().equals(serdeLib));
  }

  private static void checkFormatClass(
      String format, String property, String given, String expected) {
    Preconditions.checkArgument(
        given == null || given.equals(expected),
        "'%s' is %s, which conflicts with '%s'=%s (expected %s)",
        property,
        given,
        GlueConstants.FORMAT,
        format,
        expected);
  }

  /**
   * Resolves the storage location of a table, in order of precedence: the explicit {@code location}
   * property, the {@code LocationUri} the Glue database declares, and finally the catalog warehouse
   * path. A database location already identifies the database, so the table name is appended
   * directly to it, whereas the warehouse path is shared by all databases and needs the database
   * name in between.
   */
  private String resolveTableLocation(String explicitLocation, String dbName, String tableName) {
    // A blank location is treated as absent, so the documented fallbacks apply.
    if (StringUtils.isNotBlank(explicitLocation)) {
      return explicitLocation;
    }
    String databaseLocation = databaseLocationUri(dbName);
    if (StringUtils.isNotBlank(databaseLocation)) {
      return StringUtils.stripEnd(databaseLocation, "/") + "/" + tableName;
    }
    if (StringUtils.isNotBlank(warehouseLocation)) {
      return StringUtils.stripEnd(warehouseLocation, "/") + "/" + dbName + "/" + tableName;
    }
    throw new IllegalArgumentException(
        "Table location is required: either set the '"
            + GlueConstants.LOCATION
            + "' property or configure '"
            + GlueConstants.WAREHOUSE
            + "' on the catalog.");
  }

  /** Returns the {@code LocationUri} of the given Glue database, or null when it declares none. */
  private String databaseLocationUri(String dbName) {
    GetDatabaseRequest.Builder req = GetDatabaseRequest.builder().name(dbName);
    applyCatalogId(catalogId, req::catalogId);
    try {
      return callGlue(() -> glueClient.getDatabase(req.build()), "schema " + dbName)
          .database()
          .locationUri();
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + dbName);
    }
  }

  /** Passes {@code catalogId} to {@code setter} when it is non-null. */
  static void applyCatalogId(String catalogId, Consumer<String> setter) {
    if (catalogId != null) setter.accept(catalogId);
  }

  /**
   * Invokes a Glue SDK call and converts the failures that do not depend on the object (credential,
   * region, throttling, invalid input, access denied, timeout and transport failures) through
   * {@link GlueExceptionConverter#toClientException}. Any other {@link SdkException} is rethrown
   * unchanged so each call site's own catch block still applies its not-found/already-exists
   * semantics.
   */
  private <T> T callGlue(Supplier<T> call, String context) {
    try {
      return call.get();
    } catch (SdkException e) {
      RuntimeException converted = GlueExceptionConverter.toClientException(e, context, glueClient);
      throw converted != null ? converted : e;
    }
  }
}
