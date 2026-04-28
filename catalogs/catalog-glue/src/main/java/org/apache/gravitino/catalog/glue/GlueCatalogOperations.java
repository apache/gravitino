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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.SchemaChange;
import org.apache.gravitino.connector.CatalogInfo;
import org.apache.gravitino.connector.CatalogOperations;
import org.apache.gravitino.connector.HasPropertyMetadata;
import org.apache.gravitino.connector.SupportsSchemas;
import org.apache.gravitino.exceptions.ConnectionFailedException;
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
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.utils.PrincipalUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
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

  /** Table property keys that map to StorageDescriptor fields, not to Table.parameters(). */
  private static final Set<String> SD_TABLE_PROPERTY_KEYS =
      ImmutableSet.of(
          GlueConstants.LOCATION,
          GlueConstants.INPUT_FORMAT,
          GlueConstants.OUTPUT_FORMAT,
          GlueConstants.SERDE_LIB,
          GlueConstants.SERDE_NAME);

  /** Property keys that map to top-level TableInput fields, not to Table.parameters(). */
  private static final Set<String> TABLE_LEVEL_KEYS = ImmutableSet.of(GlueConstants.TABLE_TYPE);

  @VisibleForTesting GlueClient glueClient;

  /** Nullable — when null, Glue uses the caller's AWS account ID. */
  @VisibleForTesting String catalogId;

  /** Nullable — when null all table formats are exposed. */
  @VisibleForTesting Set<String> tableFormatFilter;

  private final GlueTypeConverter typeConverter = new GlueTypeConverter();

  @Override
  public void initialize(
      Map<String, String> config, CatalogInfo info, HasPropertyMetadata propertiesMetadata)
      throws RuntimeException {
    this.glueClient = GlueClientProvider.buildClient(config);
    this.catalogId = config.get(GlueConstants.AWS_GLUE_CATALOG_ID);
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
  }

  @Override
  public void testConnection(
      NameIdentifier catalogIdent,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws Exception {
    try {
      GetDatabasesRequest.Builder req = GetDatabasesRequest.builder().maxResults(1);
      applyCatalogId(catalogId, req::catalogId);
      glueClient.getDatabases(req.build());
    } catch (GlueException e) {
      throw new ConnectionFailedException(e, "Failed to connect to AWS Glue: %s", e.getMessage());
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
    try {
      do {
        GetDatabasesRequest.Builder req = GetDatabasesRequest.builder();
        applyCatalogId(catalogId, req::catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetDatabasesResponse resp = glueClient.getDatabases(req.build());
        resp.databaseList().stream()
            .map(db -> NameIdentifier.of(namespace, db.name()))
            .forEach(result::add);
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "listing schemas under " + namespace);
    }
    return result.toArray(new NameIdentifier[0]);
  }

  @Override
  public GlueSchema createSchema(
      NameIdentifier ident, String comment, Map<String, String> properties)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {

    Map<String, String> params = properties != null ? properties : Collections.emptyMap();

    DatabaseInput input =
        DatabaseInput.builder().name(ident.name()).description(comment).parameters(params).build();

    CreateDatabaseRequest.Builder req = CreateDatabaseRequest.builder().databaseInput(input);
    applyCatalogId(catalogId, req::catalogId);

    try {
      glueClient.createDatabase(req.build());
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + ident.name());
    }

    LOG.info("Created Glue schema (database) {}", ident.name());

    return GlueSchema.builder()
        .withName(ident.name())
        .withComment(comment)
        .withProperties(params)
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
          GlueSchema.fromGlueDatabase(glueClient.getDatabase(req.build()).database());
      LOG.info("Loaded Glue schema (database) {}", ident.name());
      return schema;
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + ident.name());
    }
  }

  @Override
  public GlueSchema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {

    GlueSchema current = loadSchema(ident);

    Map<String, String> newProps = new HashMap<>(current.properties());

    for (SchemaChange change : changes) {
      if (change instanceof SchemaChange.SetProperty) {
        SchemaChange.SetProperty sp = (SchemaChange.SetProperty) change;
        newProps.put(sp.getProperty(), sp.getValue());
      } else if (change instanceof SchemaChange.RemoveProperty) {
        newProps.remove(((SchemaChange.RemoveProperty) change).getProperty());
      } else {
        throw new IllegalArgumentException(
            "Unsupported schema change: " + change.getClass().getSimpleName());
      }
    }

    DatabaseInput input =
        DatabaseInput.builder()
            .name(ident.name())
            .description(current.comment())
            .parameters(newProps)
            .build();

    UpdateDatabaseRequest.Builder req =
        UpdateDatabaseRequest.builder().name(ident.name()).databaseInput(input);
    applyCatalogId(catalogId, req::catalogId);

    try {
      glueClient.updateDatabase(req.build());
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "schema " + ident.name());
    }

    LOG.info("Altered Glue schema (database) {}", ident.name());

    return GlueSchema.builder()
        .withName(ident.name())
        .withComment(current.comment())
        .withProperties(newProps)
        .withAuditInfo(current.auditInfo())
        .build();
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    if (!cascade) {
      GetTablesRequest.Builder tabReq =
          GetTablesRequest.builder().databaseName(ident.name()).maxResults(1);
      applyCatalogId(catalogId, tabReq::catalogId);
      try {
        if (!glueClient.getTables(tabReq.build()).tableList().isEmpty()) {
          throw new NonEmptySchemaException(
              "Schema %s is not empty. Use cascade=true to drop it with its tables.", ident.name());
        }
      } catch (GlueException e) {
        throw GlueExceptionConverter.toSchemaException(
            e, "checking tables in schema " + ident.name());
      }
    }

    DeleteDatabaseRequest.Builder req = DeleteDatabaseRequest.builder().name(ident.name());
    applyCatalogId(catalogId, req::catalogId);
    try {
      glueClient.deleteDatabase(req.build());
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
    try {
      do {
        GetTablesRequest.Builder req = GetTablesRequest.builder().databaseName(dbName);
        applyCatalogId(catalogId, req::catalogId);
        if (nextToken != null) req.nextToken(nextToken);
        GetTablesResponse resp = glueClient.getTables(req.build());
        resp.tableList().stream()
            .filter(this::matchesFormatFilter)
            .map(t -> NameIdentifier.of(namespace, t.name()))
            .forEach(result::add);
        nextToken = resp.nextToken();
      } while (nextToken != null);
    } catch (EntityNotFoundException e) {
      throw new NoSuchSchemaException(e, "Schema %s does not exist", dbName);
    } catch (GlueException e) {
      throw GlueExceptionConverter.toSchemaException(e, "listing tables in schema " + dbName);
    }
    return result.toArray(new NameIdentifier[0]);
  }

  @Override
  public GlueTable loadTable(NameIdentifier ident) throws NoSuchTableException {
    String dbName = schemaName(ident.namespace());
    GetTableRequest.Builder req = GetTableRequest.builder().databaseName(dbName).name(ident.name());
    applyCatalogId(catalogId, req::catalogId);
    try {
      GlueTable table =
          GlueTable.fromGlueTable(glueClient.getTable(req.build()).table(), typeConverter);
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

    for (Transform t : partitions) {
      Preconditions.checkArgument(
          t instanceof Transforms.IdentityTransform,
          "Glue catalog only supports identity partitioning, got: %s",
          t.name());
      Preconditions.checkArgument(
          ((Transforms.IdentityTransform) t).fieldName().length == 1,
          "Glue catalog does not support nested field partitioning");
    }

    String dbName = schemaName(ident.namespace());
    Map<String, String> props = properties != null ? properties : Collections.emptyMap();

    TableInput input =
        buildTableInput(
            ident.name(), comment, columns, props, partitions, distribution, sortOrders);

    CreateTableRequest.Builder req =
        CreateTableRequest.builder().databaseName(dbName).tableInput(input);
    applyCatalogId(catalogId, req::catalogId);

    try {
      glueClient.createTable(req.build());
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }

    LOG.info("Created Glue table {}.{}", dbName, ident.name());

    GlueTable created =
        GlueTable.builder()
            .withName(ident.name())
            .withComment(comment)
            .withColumns(columns)
            .withProperties(props)
            .withPartitioning(partitions)
            .withDistribution(distribution != null ? distribution : Distributions.NONE)
            .withSortOrders(sortOrders != null ? sortOrders : new SortOrder[0])
            .withAuditInfo(
                AuditInfo.builder()
                    .withCreator(PrincipalUtils.getCurrentUserName())
                    .withCreateTime(Instant.now())
                    .build())
            .build();
    created.initOpsContext(glueClient, catalogId, dbName);
    return created;
  }

  @Override
  public GlueTable alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {

    GlueTable current = loadTable(ident);
    String dbName = schemaName(ident.namespace());

    String newName = current.name();
    String newComment = current.comment();
    Map<String, String> newProps = new HashMap<>(current.properties());

    // Separate data columns from partition columns
    int partCount = current.partitioning().length;
    List<Column> allCols = new ArrayList<>(Arrays.asList(current.columns()));
    List<Column> dataCols = new ArrayList<>(allCols.subList(0, allCols.size() - partCount));
    List<Column> partCols =
        new ArrayList<>(allCols.subList(allCols.size() - partCount, allCols.size()));

    for (TableChange change : changes) {
      if (change instanceof TableChange.RenameTable) {
        newName = ((TableChange.RenameTable) change).getNewName();
      } else if (change instanceof TableChange.UpdateComment) {
        newComment = ((TableChange.UpdateComment) change).getNewComment();
      } else if (change instanceof TableChange.SetProperty) {
        TableChange.SetProperty sp = (TableChange.SetProperty) change;
        newProps.put(sp.getProperty(), sp.getValue());
      } else if (change instanceof TableChange.RemoveProperty) {
        newProps.remove(((TableChange.RemoveProperty) change).getProperty());
      } else if (change instanceof TableChange.ColumnChange) {
        applyColumnChange(dataCols, partCols, (TableChange.ColumnChange) change);
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change: " + change.getClass().getSimpleName());
      }
    }

    List<Column> newAllCols = new ArrayList<>(dataCols);
    newAllCols.addAll(partCols);
    Column[] newColumns = newAllCols.toArray(new Column[0]);

    TableInput input =
        buildTableInput(
            newName,
            newComment,
            newColumns,
            newProps,
            current.partitioning(),
            current.distribution(),
            current.sortOrder());

    UpdateTableRequest.Builder req =
        UpdateTableRequest.builder().databaseName(dbName).tableInput(input);
    applyCatalogId(catalogId, req::catalogId);

    try {
      glueClient.updateTable(req.build());
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }

    LOG.info("Altered Glue table {}.{}", dbName, ident.name());

    GlueTable altered =
        GlueTable.builder()
            .withName(newName)
            .withComment(newComment)
            .withColumns(newColumns)
            .withProperties(newProps)
            .withPartitioning(current.partitioning())
            .withDistribution(current.distribution())
            .withSortOrders(current.sortOrder())
            .withAuditInfo(current.auditInfo())
            .build();
    altered.initOpsContext(glueClient, catalogId, dbName);
    return altered;
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    String dbName = schemaName(ident.namespace());
    DeleteTableRequest.Builder req =
        DeleteTableRequest.builder().databaseName(dbName).name(ident.name());
    applyCatalogId(catalogId, req::catalogId);
    try {
      glueClient.deleteTable(req.build());
      LOG.info("Dropped Glue table {}.{}", dbName, ident.name());
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (GlueException e) {
      throw GlueExceptionConverter.toTableException(e, "table " + ident.name());
    }
  }

  private static String schemaName(Namespace namespace) {
    String[] levels = namespace.levels();
    Preconditions.checkArgument(
        levels.length >= 2, "Namespace must have at least 2 levels, got: %s", levels.length);
    return levels[levels.length - 1];
  }

  // NOTE: parameter type is the Glue SDK Table, not GlueTable (our domain class).
  // The Glue SDK's Column model is also referenced by FQN throughout this class because its
  // simple name conflicts with the imported org.apache.gravitino.rel.Column.
  private boolean matchesFormatFilter(Table table) {
    if (tableFormatFilter == null) return true;
    String fmt = table.hasParameters() ? table.parameters().get(GlueConstants.TABLE_FORMAT) : null;
    String normalized =
        fmt != null ? fmt.toLowerCase(Locale.ROOT) : GlueConstants.DEFAULT_TABLE_FORMAT_VALUE;
    return tableFormatFilter.contains(normalized);
  }

  private TableInput buildTableInput(
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
      } else if (!SD_TABLE_PROPERTY_KEYS.contains(key) && !TABLE_LEVEL_KEYS.contains(key)) {
        tableParams.put(key, entry.getValue());
      }
    }

    SerDeInfo serDe =
        SerDeInfo.builder()
            .serializationLibrary(properties.get(GlueConstants.SERDE_LIB))
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
              .collect(Collectors.toList());
    }

    List<Order> glueSortCols = new ArrayList<>();
    if (sortOrders != null) {
      for (SortOrder so : sortOrders) {
        if (so.expression() instanceof NamedReference.FieldReference) {
          String colName =
              String.join(".", ((NamedReference.FieldReference) so.expression()).fieldName());
          int order = so.direction() == SortDirection.ASCENDING ? 1 : 0;
          glueSortCols.add(Order.builder().column(colName).sortOrder(order).build());
        }
      }
    }

    StorageDescriptor sd =
        StorageDescriptor.builder()
            .columns(glueDataCols)
            .location(properties.get(GlueConstants.LOCATION))
            .inputFormat(properties.get(GlueConstants.INPUT_FORMAT))
            .outputFormat(properties.get(GlueConstants.OUTPUT_FORMAT))
            .serdeInfo(serDe)
            .bucketColumns(bucketCols)
            .numberOfBuckets(numBuckets)
            .sortColumns(glueSortCols)
            .build();

    return TableInput.builder()
        .name(name)
        .description(comment)
        .tableType(properties.get(GlueConstants.TABLE_TYPE))
        .parameters(tableParams)
        .storageDescriptor(sd)
        .partitionKeys(gluePartCols)
        .build();
  }

  private software.amazon.awssdk.services.glue.model.Column toGlueColumn(Column col) {
    return software.amazon.awssdk.services.glue.model.Column.builder()
        .name(col.name())
        .type(typeConverter.fromGravitino(col.dataType()))
        .comment(col.comment())
        .build();
  }

  private static void applyColumnChange(
      List<Column> dataCols, List<Column> partCols, TableChange.ColumnChange change) {

    String fieldName = change.fieldName()[0];

    if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn add = (TableChange.AddColumn) change;
      dataCols.add(
          GlueColumn.builder()
              .withName(add.fieldName()[0])
              .withType(add.getDataType())
              .withComment(add.getComment())
              .withNullable(add.isNullable())
              .build());

    } else if (change instanceof TableChange.DeleteColumn) {
      boolean isPartition = partCols.stream().anyMatch(c -> c.name().equals(fieldName));
      Preconditions.checkArgument(!isPartition, "Cannot delete partition column: %s", fieldName);
      dataCols.removeIf(c -> c.name().equals(fieldName));

    } else if (change instanceof TableChange.RenameColumn) {
      boolean isPartition = partCols.stream().anyMatch(c -> c.name().equals(fieldName));
      Preconditions.checkArgument(!isPartition, "Cannot rename partition column: %s", fieldName);
      String newColName = ((TableChange.RenameColumn) change).getNewName();
      replaceColumn(
          dataCols, fieldName, old -> copyColumn(old, newColName, old.dataType(), old.comment()));

    } else if (change instanceof TableChange.UpdateColumnType) {
      boolean isPartition = partCols.stream().anyMatch(c -> c.name().equals(fieldName));
      Preconditions.checkArgument(
          !isPartition, "Cannot update type of partition column: %s", fieldName);
      Type newType = ((TableChange.UpdateColumnType) change).getNewDataType();
      replaceColumn(
          dataCols, fieldName, old -> copyColumn(old, old.name(), newType, old.comment()));

    } else if (change instanceof TableChange.UpdateColumnComment) {
      String newCmt = ((TableChange.UpdateColumnComment) change).getNewComment();
      if (!replaceColumn(
          dataCols, fieldName, old -> copyColumn(old, old.name(), old.dataType(), newCmt))) {
        replaceColumn(
            partCols, fieldName, old -> copyColumn(old, old.name(), old.dataType(), newCmt));
      }

    } else {
      throw new IllegalArgumentException(
          "Unsupported column change: " + change.getClass().getSimpleName());
    }
  }

  /** Passes {@code catalogId} to {@code setter} when it is non-null. */
  static void applyCatalogId(String catalogId, Consumer<String> setter) {
    if (catalogId != null) setter.accept(catalogId);
  }

  /**
   * Finds the first column matching {@code name} in {@code cols}, replaces it with the result of
   * {@code updater}, and returns {@code true} if a replacement was made.
   */
  private static boolean replaceColumn(
      List<Column> cols, String name, UnaryOperator<Column> updater) {
    for (int i = 0; i < cols.size(); i++) {
      if (cols.get(i).name().equals(name)) {
        cols.set(i, updater.apply(cols.get(i)));
        return true;
      }
    }
    return false;
  }

  private static Column copyColumn(Column src, String name, Type type, String comment) {
    return GlueColumn.builder()
        .withName(name)
        .withType(type)
        .withComment(comment)
        .withNullable(src.nullable())
        .build();
  }
}
