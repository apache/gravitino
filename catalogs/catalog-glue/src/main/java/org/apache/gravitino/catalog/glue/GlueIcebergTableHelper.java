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

import static org.apache.gravitino.rel.types.Types.BinaryType;
import static org.apache.gravitino.rel.types.Types.BooleanType;
import static org.apache.gravitino.rel.types.Types.ByteType;
import static org.apache.gravitino.rel.types.Types.DateType;
import static org.apache.gravitino.rel.types.Types.DecimalType;
import static org.apache.gravitino.rel.types.Types.DoubleType;
import static org.apache.gravitino.rel.types.Types.ExternalType;
import static org.apache.gravitino.rel.types.Types.FixedCharType;
import static org.apache.gravitino.rel.types.Types.FixedType;
import static org.apache.gravitino.rel.types.Types.FloatType;
import static org.apache.gravitino.rel.types.Types.GeographyType;
import static org.apache.gravitino.rel.types.Types.GeometryType;
import static org.apache.gravitino.rel.types.Types.IntegerType;
import static org.apache.gravitino.rel.types.Types.ListType;
import static org.apache.gravitino.rel.types.Types.LongType;
import static org.apache.gravitino.rel.types.Types.MapType;
import static org.apache.gravitino.rel.types.Types.NullType;
import static org.apache.gravitino.rel.types.Types.ShortType;
import static org.apache.gravitino.rel.types.Types.StringType;
import static org.apache.gravitino.rel.types.Types.StructType;
import static org.apache.gravitino.rel.types.Types.TimeType;
import static org.apache.gravitino.rel.types.Types.TimestampType;
import static org.apache.gravitino.rel.types.Types.UUIDType;
import static org.apache.gravitino.rel.types.Types.VarCharType;
import static org.apache.gravitino.rel.types.Types.VariantType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Type;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.transforms.SortOrderVisitor;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper that delegates all Iceberg table operations to the Iceberg SDK's {@code GlueCatalog}.
 *
 * <p>Unlike the native AWS Glue SDK {@code OpenTableFormatInput} approach, the Iceberg SDK writes
 * the {@code metadata.json} file to S3 and registers the table in Glue with the correct {@code
 * metadata_location} parameter, making the table usable by Trino Lakehouse connector and other
 * Iceberg-native query engines.
 */
final class GlueIcebergTableHelper {

  /** Name of the internal Iceberg catalog; it never reaches users. */
  static final String ICEBERG_CATALOG_NAME = "gravitino-glue-iceberg";

  private static final Logger LOG = LoggerFactory.getLogger(GlueIcebergTableHelper.class);

  private static final String DOT = ".";

  private static final int MICROSECOND_PRECISION = 6;

  private static final int NANOSECOND_PRECISION = 9;

  private static final Set<String> S3_SCHEMES = ImmutableSet.of("s3", "s3a", "s3n");

  // Iceberg GlueCatalog properties (not defined in IcebergConstants).
  private static final String CATALOG_IMPL = "catalog-impl";
  private static final String GLUE_CATALOG = "GlueCatalog";
  private static final String GLUE_ID = "glue.id";
  private static final String GLUE_ENDPOINT = "glue.endpoint";
  private static final String HTTP_CLIENT_TYPE = "http-client.type";
  private static final String HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS =
      "http-client.apache.connection-timeout-ms";
  private static final String HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS =
      "http-client.apache.socket-timeout-ms";
  private static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
  private static final String CLIENT_CREDENTIALS_PROVIDER_ACCESS_KEY_ID =
      "client.credentials-provider.access-key-id";
  private static final String CLIENT_CREDENTIALS_PROVIDER_SECRET_ACCESS_KEY =
      "client.credentials-provider.secret-access-key";

  /** Catalog properties the Glue client of the Iceberg catalog is built from. */
  private static final Set<String> GLUE_CLIENT_CONFIG_KEYS =
      ImmutableSet.of(
          GlueConstants.AWS_REGION,
          GlueConstants.AWS_ACCESS_KEY_ID,
          GlueConstants.AWS_SECRET_ACCESS_KEY,
          GlueConstants.AWS_GLUE_ENDPOINT,
          GlueConstants.AWS_GLUE_API_CALL_TIMEOUT_MS,
          GlueConstants.AWS_GLUE_API_CALL_ATTEMPT_TIMEOUT_MS,
          GlueConstants.AWS_GLUE_MAX_ATTEMPTS);

  /** Table properties that Glue holds about the table, which Iceberg properties cannot shadow. */
  private static final Set<String> GLUE_OWNED_PROPS =
      ImmutableSet.of(
          GlueConstants.TABLE_TYPE_PARAM,
          GlueConstants.TABLE_FORMAT,
          GlueConstants.METADATA_LOCATION,
          GlueConstants.REGISTERED_ICEBERG_TABLE_PARAM);

  private static final Set<String> EXCLUDED_TABLE_PROPS =
      Set.of(
          GlueConstants.LOCATION,
          GlueConstants.METADATA_LOCATION,
          GlueConstants.INPUT_FORMAT_CLASS,
          GlueConstants.OUTPUT_FORMAT,
          GlueConstants.SERDE_LIB);

  private GlueIcebergTableHelper() {}

  /**
   * Returns true if the Glue table is an Iceberg-format table.
   *
   * <p>Checks for {@code table_type=ICEBERG} in {@code Table.parameters()}.
   */
  static boolean isIcebergTable(software.amazon.awssdk.services.glue.model.Table glueTable) {
    Preconditions.checkArgument(glueTable != null, "glueTable cannot be null");
    if (!glueTable.hasParameters()) return false;
    return GlueConstants.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
        glueTable.parameters().get(GlueConstants.TABLE_TYPE_PARAM));
  }

  /**
   * Creates an Iceberg {@link Catalog} backed by AWS Glue.
   *
   * @param config Gravitino catalog configuration (region, credentials, endpoint, etc.). Must
   *     contain {@code aws-region} and {@code warehouse}.
   * @return an initialized Iceberg Glue catalog
   * @throws IllegalArgumentException if {@code aws-region} or {@code warehouse} is not configured
   */
  static Catalog createGlueCatalog(Map<String, String> config) {
    Map<String, String> icebergProps = buildIcebergCatalogProperties(config);

    GlueCatalog glueCatalog = new GlueCatalog();
    glueCatalog.initialize(ICEBERG_CATALOG_NAME, icebergProps);
    LOG.info("Initialized Iceberg GlueCatalog for region {}", config.get(GlueConstants.AWS_REGION));
    return glueCatalog;
  }

  /**
   * Whether {@code location} is an S3 URI with a bucket, the only kind of location S3FileIO can
   * write Iceberg metadata to.
   *
   * @param location the table location
   * @return true for an {@code s3://}, {@code s3a://} or {@code s3n://} URI with a bucket
   */
  static boolean isS3Location(String location) {
    try {
      URI uri = new URI(location);
      return uri.getScheme() != null
          && S3_SCHEMES.contains(uri.getScheme().toLowerCase(Locale.ROOT))
          && StringUtils.isNotBlank(uri.getRawAuthority());
    } catch (URISyntaxException e) {
      return false;
    }
  }

  static Map<String, String> buildIcebergCatalogProperties(Map<String, String> config) {
    String region = config.get(GlueConstants.AWS_REGION);
    Preconditions.checkArgument(region != null, "AWS region is required for Iceberg Glue catalog");

    String warehouse = config.get(GlueConstants.WAREHOUSE);
    Preconditions.checkArgument(
        warehouse != null,
        "Warehouse is required for Iceberg Glue catalog; configure '%s' on the catalog.",
        GlueConstants.WAREHOUSE);

    Map<String, String> icebergProps = new HashMap<>();
    icebergProps.put(IcebergConstants.WAREHOUSE, warehouse);
    icebergProps.put(CATALOG_IMPL, GLUE_CATALOG);
    icebergProps.put(IcebergConstants.AWS_S3_REGION, region);

    String catalogId = config.get(GlueConstants.AWS_GLUE_CATALOG_ID);
    if (StringUtils.isNotBlank(catalogId)) {
      icebergProps.put(GLUE_ID, catalogId);
    }

    String accessKey = config.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretKey = config.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    boolean hasCredential = GlueClientProvider.hasAwsStaticCredentials(accessKey, secretKey);
    if (hasCredential) {
      icebergProps.put(
          CLIENT_CREDENTIALS_PROVIDER, GravitinoGlueCredentialsProvider.class.getName());
      icebergProps.put(CLIENT_CREDENTIALS_PROVIDER_ACCESS_KEY_ID, accessKey);
      icebergProps.put(CLIENT_CREDENTIALS_PROVIDER_SECRET_ACCESS_KEY, secretKey);
      icebergProps.put(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID, accessKey);
      icebergProps.put(IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY, secretKey);
    }

    String endpoint = config.get(GlueConstants.AWS_GLUE_ENDPOINT);
    if (StringUtils.isNotBlank(endpoint)) {
      icebergProps.put(GLUE_ENDPOINT, endpoint);
    }

    // The Glue client is built like the catalog's own, with its call timeout and retries.
    icebergProps.put(AwsProperties.CLIENT_FACTORY, GlueIcebergAwsClientFactory.class.getName());
    GLUE_CLIENT_CONFIG_KEYS.forEach(
        key -> {
          String value = config.get(key);
          if (value != null) {
            icebergProps.put(GlueIcebergAwsClientFactory.GLUE_CONFIG_PREFIX + key, value);
          }
        });

    // The Iceberg S3 client takes no total call timeout, so bound each connection and read by the
    // per-attempt timeout of the Glue client.
    String attemptTimeoutMs = String.valueOf(GlueClientProvider.apiCallAttemptTimeoutMs(config));
    icebergProps.put(HTTP_CLIENT_TYPE, "apache");
    icebergProps.put(HTTP_CLIENT_APACHE_CONNECTION_TIMEOUT_MS, attemptTimeoutMs);
    icebergProps.put(HTTP_CLIENT_APACHE_SOCKET_TIMEOUT_MS, attemptTimeoutMs);

    icebergProps.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    return icebergProps;
  }

  /**
   * Loads an Iceberg table and recovers partitioning and sort orders.
   *
   * @param icebergCatalog the Iceberg Glue catalog
   * @param dbName the Glue database name
   * @param tableName the table name
   * @param table the GlueTable to populate with recovered metadata
   */
  static void loadTable(Catalog icebergCatalog, String dbName, String tableName, GlueTable table) {
    Table icebergTable = icebergCatalog.loadTable(TableIdentifier.of(dbName, tableName));
    if (icebergTable == null) {
      throw new NoSuchTableException(
          "Iceberg table %s.%s not found in Iceberg catalog", dbName, tableName);
    }

    // Merge Iceberg table properties (stored in metadata.json) into the Gravitino properties.
    // Iceberg properties take precedence over Glue parameters for overlapping keys, except for the
    // ones Glue holds about the table itself, such as its format and metadata file.
    Map<String, String> mergedProps = new HashMap<>(table.properties());
    icebergTable
        .properties()
        .forEach(
            (key, value) -> {
              if (!GLUE_OWNED_PROPS.contains(key)) {
                mergedProps.put(key, value);
              }
            });
    table.setProperties(mergedProps);

    // Overwrite Glue's Hive-style column types with the accurate types from the Iceberg schema.
    // Glue stores Iceberg TIME as "string", REAL as "float", etc., so the Iceberg schema is the
    // authoritative source for column types on Iceberg tables.
    Schema icebergSchema = icebergTable.schema();
    Column[] columns =
        icebergSchema.columns().stream()
            .map(
                field ->
                    GlueColumn.builder()
                        .withName(field.name())
                        .withType(fromIcebergType(field.type()))
                        .withComment(field.doc())
                        .withNullable(field.isOptional())
                        .build())
            .toArray(Column[]::new);
    table.setColumns(columns);

    if (!icebergTable.spec().fields().isEmpty()) {
      table.setPartitioning(convertPartitionSpec(icebergTable.spec(), icebergTable.schema()));
    }
    if (!icebergTable.sortOrder().fields().isEmpty()) {
      table.setSortOrders(convertIcebergSortOrder(icebergTable.sortOrder(), icebergTable.schema()));
    }
  }

  /**
   * Converts an Iceberg type to the equivalent Gravitino type.
   *
   * <p>TIME and TIMESTAMP types are returned with microsecond (6) precision and the nanosecond
   * timestamp types with precision 9, as the Iceberg catalog reports them.
   */
  // TODO: the Iceberg-to-Gravitino conversions in this class (type mapping, partition spec,
  // sort order, etc.) duplicate logic in catalog-lakehouse-iceberg and must give the same results.
  // Consider extracting them to a shared layer (e.g. catalog-common) so both catalogs can reuse
  // the code.
  // The parameter uses FQN because org.apache.iceberg.types.Type and
  // org.apache.gravitino.rel.types.Type share the same simple name.
  static Type fromIcebergType(org.apache.iceberg.types.Type icebergType) {
    switch (icebergType.typeId()) {
      case BOOLEAN:
        return BooleanType.get();
      case INTEGER:
        return IntegerType.get();
      case LONG:
        return LongType.get();
      case FLOAT:
        return FloatType.get();
      case DOUBLE:
        return DoubleType.get();
      case STRING:
        return StringType.get();
      case BINARY:
        return BinaryType.get();
      case UUID:
        return UUIDType.get();
      case DATE:
        return DateType.get();
      case TIME:
        return TimeType.of(MICROSECOND_PRECISION);
      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) icebergType;
        return ts.shouldAdjustToUTC()
            ? TimestampType.withTimeZone(MICROSECOND_PRECISION)
            : TimestampType.withoutTimeZone(MICROSECOND_PRECISION);
      case TIMESTAMP_NANO:
        Types.TimestampNanoType tsNano = (Types.TimestampNanoType) icebergType;
        return tsNano.shouldAdjustToUTC()
            ? TimestampType.withTimeZone(NANOSECOND_PRECISION)
            : TimestampType.withoutTimeZone(NANOSECOND_PRECISION);
      case VARIANT:
        return VariantType.get();
      case UNKNOWN:
        // Iceberg v3 unknown is the null-only placeholder type.
        return NullType.get();
      case GEOMETRY:
        return GeometryType.of(((Types.GeometryType) icebergType).crs());
      case GEOGRAPHY:
        Types.GeographyType geography = (Types.GeographyType) icebergType;
        // EdgeAlgorithm.toString() is the lower-case spec name, which GeographyType requires.
        return GeographyType.of(geography.crs(), geography.algorithm().toString());
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) icebergType;
        return DecimalType.of(decimal.precision(), decimal.scale());
      case FIXED:
        Types.FixedType fixed = (Types.FixedType) icebergType;
        return FixedType.of(fixed.length());
      case LIST:
        Types.ListType list = (Types.ListType) icebergType;
        return ListType.of(fromIcebergType(list.elementType()), list.isElementOptional());
      case MAP:
        Types.MapType map = (Types.MapType) icebergType;
        return MapType.of(
            fromIcebergType(map.keyType()),
            fromIcebergType(map.valueType()),
            map.isValueOptional());
      case STRUCT:
        Types.StructType struct = (Types.StructType) icebergType;
        StructType.Field[] fields =
            struct.fields().stream()
                .map(
                    f ->
                        StructType.Field.of(
                            f.name(), fromIcebergType(f.type()), f.isOptional(), f.doc()))
                .toArray(StructType.Field[]::new);
        return StructType.of(fields);
      default:
        return ExternalType.of(icebergType.typeId().name());
    }
  }

  /**
   * Creates an Iceberg table via the Iceberg SDK.
   *
   * <p>The table is written to S3 (metadata.json) and registered in Glue automatically.
   *
   * @param icebergCatalog the Iceberg Glue catalog
   * @param dbName the Glue database name
   * @param tableName the table name
   * @param columns the table columns
   * @param comment the table comment
   * @param properties Gravitino table properties (must contain {@code location})
   * @param partitions partition transforms
   * @param sortOrders sort orders
   */
  static void createTable(
      Catalog icebergCatalog,
      String dbName,
      String tableName,
      Column[] columns,
      @Nullable String comment,
      Map<String, String> properties,
      @Nullable Transform[] partitions,
      @Nullable SortOrder[] sortOrders) {

    Schema schema = toIcebergSchema(columns);
    PartitionSpec spec = toPartitionSpec(schema, partitions);
    org.apache.iceberg.SortOrder icebergSortOrder = toSortOrder(schema, sortOrders);

    Map<String, String> tableProps = new HashMap<>();
    // Forward user-provided properties to Iceberg, excluding Gravitino/Glue internal keys.
    properties.forEach(
        (k, v) -> {
          if (!EXCLUDED_TABLE_PROPS.contains(k)) {
            tableProps.put(k, v);
          }
        });
    if (comment != null) {
      tableProps.put("comment", comment);
    }

    String location = properties.get(GlueConstants.LOCATION);
    Preconditions.checkArgument(
        location != null, "Location is required for Iceberg table creation");
    // Iceberg metadata is written through S3FileIO only, so any other scheme can never work.
    Preconditions.checkArgument(
        isS3Location(location),
        "Iceberg tables need an s3://, s3a:// or s3n:// location; got '%s'. Set '%s' on the"
            + " table, or an S3 '%s' on the catalog or LocationUri on the database.",
        location,
        GlueConstants.LOCATION,
        GlueConstants.WAREHOUSE);

    String format = properties.get(GlueConstants.FORMAT);
    if (format != null) {
      tableProps.put("write.format.default", format.toLowerCase(Locale.ROOT));
    }

    TableIdentifier tableId = TableIdentifier.of(dbName, tableName);

    LOG.info("Creating Iceberg table {} at location {} via Iceberg SDK", tableId, location);

    try {
      icebergCatalog
          .buildTable(tableId, schema)
          .withLocation(location)
          .withPartitionSpec(spec)
          .withSortOrder(icebergSortOrder)
          .withProperties(tableProps)
          .create();
    } catch (AlreadyExistsException e) {
      throw new org.apache.gravitino.exceptions.TableAlreadyExistsException(
          e, "Table %s.%s already exists", dbName, tableName);
    } catch (ValidationException e) {
      throw new IllegalArgumentException("Invalid table definition: " + e.getMessage(), e);
    }
  }

  /**
   * Alters an Iceberg table via the Iceberg SDK.
   *
   * <p>Supports {@link TableChange.RenameTable}, {@link TableChange.ColumnChange}, {@link
   * TableChange.SetProperty}, and {@link TableChange.RemoveProperty}. A rename is an exclusive
   * operation and cannot be combined with other changes.
   *
   * <p>Schema and property changes are committed in one Iceberg transaction. Column changes that
   * Iceberg cannot apply, such as auto-increment or default values, are refused before anything is
   * staged.
   *
   * @param icebergCatalog the Iceberg Glue catalog
   * @param dbName the Glue database name
   * @param tableName the table name
   * @param changes the table changes to apply
   * @return the final {@link TableIdentifier} (differs from the input only when a rename is
   *     applied, including cross-schema renames)
   */
  static TableIdentifier alterTable(
      Catalog icebergCatalog, String dbName, String tableName, TableChange... changes) {
    for (TableChange change : changes) {
      if (change instanceof TableChange.RenameTable) {
        Preconditions.checkArgument(
            changes.length == 1, "RenameTable cannot be combined with other table changes");
        TableChange.RenameTable rename = (TableChange.RenameTable) change;
        String newDbName = rename.getNewSchemaName().orElse(dbName);
        String newName = rename.getNewName();
        TableIdentifier to = TableIdentifier.of(newDbName, newName);
        icebergCatalog.renameTable(TableIdentifier.of(dbName, tableName), to);
        LOG.info("Renamed Iceberg table {}.{} to {}.{}", dbName, tableName, newDbName, newName);
        return to;
      }
    }

    boolean hasSchemaChange = false;
    boolean hasPropChange = false;
    for (TableChange change : changes) {
      if (change instanceof TableChange.ColumnChange) {
        checkColumnChange((TableChange.ColumnChange) change);
        hasSchemaChange = true;
      } else if (change instanceof TableChange.SetProperty
          || change instanceof TableChange.RemoveProperty) {
        hasPropChange = true;
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change for Iceberg table: " + change.getClass().getSimpleName());
      }
    }

    Table table = icebergCatalog.loadTable(TableIdentifier.of(dbName, tableName));
    // Schema and property changes are staged in one transaction, so a request is applied as a
    // whole or not at all.
    Transaction transaction = table.newTransaction();

    if (hasSchemaChange) {
      UpdateSchema update = transaction.updateSchema();
      for (TableChange change : changes) {
        if (change instanceof TableChange.AddColumn) {
          TableChange.AddColumn add = (TableChange.AddColumn) change;
          String name = add.fieldName()[0];
          update.addColumn(name, toIcebergType(add.getDataType()), add.getComment());
          if (!add.isNullable()) {
            update.requireColumn(name);
          }
          moveColumn(update, table.schema(), name, add.getPosition());
        } else if (change instanceof TableChange.UpdateColumnPosition) {
          TableChange.UpdateColumnPosition move = (TableChange.UpdateColumnPosition) change;
          moveColumn(update, table.schema(), move.fieldName()[0], move.getPosition());
        } else if (change instanceof TableChange.DeleteColumn) {
          TableChange.DeleteColumn del = (TableChange.DeleteColumn) change;
          Preconditions.checkArgument(
              del.fieldName().length == 1, "Nested column deletions are not supported");
          if (del.getIfExists()) {
            try {
              update.deleteColumn(del.fieldName()[0]);
            } catch (IllegalArgumentException e) {
              // Column does not exist; ignore as requested by ifExists=true.
            }
          } else {
            update.deleteColumn(del.fieldName()[0]);
          }
        } else if (change instanceof TableChange.RenameColumn) {
          TableChange.RenameColumn rename = (TableChange.RenameColumn) change;
          Preconditions.checkArgument(
              rename.fieldName().length == 1, "Nested column renames are not supported");
          update.renameColumn(rename.fieldName()[0], rename.getNewName());
        } else if (change instanceof TableChange.UpdateColumnType) {
          TableChange.UpdateColumnType upd = (TableChange.UpdateColumnType) change;
          Preconditions.checkArgument(
              upd.fieldName().length == 1, "Nested column type updates are not supported");
          org.apache.iceberg.types.Type newIcebergType = toIcebergType(upd.getNewDataType());
          Preconditions.checkArgument(
              newIcebergType instanceof org.apache.iceberg.types.Type.PrimitiveType,
              "Iceberg only supports primitive type promotion via updateColumn, got: %s",
              upd.getNewDataType().simpleString());
          update.updateColumn(
              upd.fieldName()[0], (org.apache.iceberg.types.Type.PrimitiveType) newIcebergType);
        } else if (change instanceof TableChange.UpdateColumnComment) {
          TableChange.UpdateColumnComment upd = (TableChange.UpdateColumnComment) change;
          Preconditions.checkArgument(
              upd.fieldName().length == 1, "Nested column comment updates are not supported");
          update.updateColumnDoc(upd.fieldName()[0], upd.getNewComment());
        } else if (change instanceof TableChange.UpdateColumnNullability) {
          TableChange.UpdateColumnNullability upd = (TableChange.UpdateColumnNullability) change;
          Preconditions.checkArgument(
              upd.fieldName().length == 1, "Nested column nullability updates are not supported");
          if (upd.nullable()) {
            update.makeColumnOptional(upd.fieldName()[0]);
          } else {
            update.requireColumn(upd.fieldName()[0]);
          }
        }
      }
      update.commit();
    }

    if (hasPropChange) {
      UpdateProperties update = transaction.updateProperties();
      for (TableChange change : changes) {
        if (change instanceof TableChange.SetProperty) {
          TableChange.SetProperty sp = (TableChange.SetProperty) change;
          update.set(sp.getProperty(), sp.getValue());
        } else if (change instanceof TableChange.RemoveProperty) {
          TableChange.RemoveProperty rp = (TableChange.RemoveProperty) change;
          update.remove(rp.getProperty());
        }
      }
      update.commit();
    }

    transaction.commitTransaction();
    LOG.info("Altered Iceberg table {}.{} via Iceberg SDK", dbName, tableName);
    return TableIdentifier.of(dbName, tableName);
  }

  /**
   * Refuses column changes that Iceberg cannot apply through this catalog, before anything is
   * staged, so that no change of a request is skipped without an error.
   */
  private static void checkColumnChange(TableChange.ColumnChange change) {
    String[] fieldName = change.fieldName();
    Preconditions.checkArgument(
        fieldName != null && fieldName.length > 0, "Column change field name cannot be empty");
    String name = String.join(DOT, fieldName);
    Preconditions.checkArgument(
        fieldName.length == 1,
        "Iceberg tables in Glue do not support changes to nested field %s",
        name);
    if (change instanceof TableChange.AddColumn) {
      TableChange.AddColumn add = (TableChange.AddColumn) change;
      Preconditions.checkArgument(
          !add.isAutoIncrement(), "Iceberg does not support auto-increment columns: %s", name);
      Preconditions.checkArgument(
          add.getDefaultValue() == null
              || Column.DEFAULT_VALUE_NOT_SET.equals(add.getDefaultValue()),
          "Iceberg tables in Glue do not support column default values: %s",
          name);
      return;
    }
    Preconditions.checkArgument(
        change instanceof TableChange.UpdateColumnPosition
            || change instanceof TableChange.DeleteColumn
            || change instanceof TableChange.RenameColumn
            || change instanceof TableChange.UpdateColumnType
            || change instanceof TableChange.UpdateColumnComment
            || change instanceof TableChange.UpdateColumnNullability,
        "Iceberg tables in Glue do not support %s on column %s",
        change.getClass().getSimpleName(),
        name);
  }

  /**
   * Moves a column to the requested position. The default position leaves an added column at the
   * end and moves an existing column after the last column.
   */
  private static void moveColumn(
      UpdateSchema update, Schema schema, String name, TableChange.ColumnPosition position) {
    if (position instanceof TableChange.First) {
      update.moveFirst(name);
    } else if (position instanceof TableChange.After) {
      update.moveAfter(name, ((TableChange.After) position).getColumn());
    } else if (schema.findField(name) != null) {
      List<Types.NestedField> columns = schema.columns();
      String last = columns.get(columns.size() - 1).name();
      if (!last.equals(name)) {
        update.moveAfter(name, last);
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Partition / sort conversion (Iceberg -> Gravitino)
  // ---------------------------------------------------------------------------

  /**
   * Converts an Iceberg {@link org.apache.iceberg.PartitionSpec} to Gravitino {@link Transform}s.
   *
   * <p>Fields with the {@code void} transform, which Iceberg v1 tables keep for dropped partition
   * fields, no longer partition data and are left out. A transform Gravitino has no type for is
   * reported by its Iceberg name.
   */
  static Transform[] convertPartitionSpec(PartitionSpec spec, Schema schema) {
    PartitionSpecVisitor<Transform> visitor =
        new PartitionSpecVisitor<Transform>() {
          @Override
          public Transform identity(int fieldId, String sourceName, int sourceId) {
            return Transforms.identity(columnName(schema, sourceId));
          }

          @Override
          public Transform bucket(int fieldId, String sourceName, int sourceId, int numBuckets) {
            return Transforms.bucket(numBuckets, new String[] {columnName(schema, sourceId)});
          }

          @Override
          public Transform truncate(int fieldId, String sourceName, int sourceId, int width) {
            return Transforms.truncate(width, columnName(schema, sourceId));
          }

          @Override
          public Transform year(int fieldId, String sourceName, int sourceId) {
            return Transforms.year(columnName(schema, sourceId));
          }

          @Override
          public Transform month(int fieldId, String sourceName, int sourceId) {
            return Transforms.month(columnName(schema, sourceId));
          }

          @Override
          public Transform day(int fieldId, String sourceName, int sourceId) {
            return Transforms.day(columnName(schema, sourceId));
          }

          @Override
          public Transform hour(int fieldId, String sourceName, int sourceId) {
            return Transforms.hour(columnName(schema, sourceId));
          }

          @Override
          public Transform alwaysNull(int fieldId, String sourceName, int sourceId) {
            return null;
          }

          @Override
          public Transform unknown(int fieldId, String sourceName, int sourceId, String transform) {
            return Transforms.apply(
                transform, new Expression[] {NamedReference.field(columnName(schema, sourceId))});
          }
        };
    return spec.fields().stream()
        .map(field -> PartitionSpecVisitor.visit(schema, field, visitor))
        .filter(Objects::nonNull)
        .toArray(Transform[]::new);
  }

  /**
   * Converts an Iceberg {@link org.apache.iceberg.SortOrder} to Gravitino {@link SortOrder}s. A
   * transform Gravitino has no function for is reported by its Iceberg name.
   */
  static SortOrder[] convertIcebergSortOrder(
      org.apache.iceberg.SortOrder iceSortOrder, Schema schema) {
    if (iceSortOrder == null || iceSortOrder.fields().isEmpty()) {
      return new SortOrder[0];
    }
    SortOrderVisitor<SortOrder> visitor =
        new SortOrderVisitor<SortOrder>() {
          @Override
          public SortOrder field(
              String sourceName,
              int sourceId,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return sortOrder(column(sourceId), direction, nullOrder);
          }

          @Override
          public SortOrder bucket(
              String sourceName,
              int sourceId,
              int numBuckets,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return sortOrder(
                FunctionExpression.of(
                    "bucket", Literals.integerLiteral(numBuckets), column(sourceId)),
                direction,
                nullOrder);
          }

          @Override
          public SortOrder truncate(
              String sourceName,
              int sourceId,
              int width,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return sortOrder(
                FunctionExpression.of("truncate", Literals.integerLiteral(width), column(sourceId)),
                direction,
                nullOrder);
          }

          @Override
          public SortOrder year(
              String sourceName,
              int sourceId,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return function("year", sourceId, direction, nullOrder);
          }

          @Override
          public SortOrder month(
              String sourceName,
              int sourceId,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return function("month", sourceId, direction, nullOrder);
          }

          @Override
          public SortOrder day(
              String sourceName,
              int sourceId,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return function("day", sourceId, direction, nullOrder);
          }

          @Override
          public SortOrder hour(
              String sourceName,
              int sourceId,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return function("hour", sourceId, direction, nullOrder);
          }

          @Override
          public SortOrder unknown(
              String sourceName,
              int sourceId,
              String transform,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return function(transform, sourceId, direction, nullOrder);
          }

          private SortOrder function(
              String name,
              int sourceId,
              org.apache.iceberg.SortDirection direction,
              NullOrder nullOrder) {
            return sortOrder(FunctionExpression.of(name, column(sourceId)), direction, nullOrder);
          }

          private NamedReference column(int sourceId) {
            return NamedReference.field(columnName(schema, sourceId));
          }
        };
    return SortOrderVisitor.visit(iceSortOrder, visitor).toArray(new SortOrder[0]);
  }

  private static SortOrder sortOrder(
      Expression expression, org.apache.iceberg.SortDirection direction, NullOrder nullOrder) {
    return SortOrders.of(
        expression,
        direction == org.apache.iceberg.SortDirection.ASC
            ? SortDirection.ASCENDING
            : SortDirection.DESCENDING,
        nullOrder == NullOrder.NULLS_FIRST ? NullOrdering.NULLS_FIRST : NullOrdering.NULLS_LAST);
  }

  // ---------------------------------------------------------------------------
  // Type conversion (Gravitino -> Iceberg)
  // ---------------------------------------------------------------------------

  @VisibleForTesting
  static Schema toIcebergSchema(Column[] columns) {
    List<Types.NestedField> fields = new ArrayList<>();
    // Top-level columns use ordinal IDs (0, 1, ...). Nested field IDs start after that range
    // so all IDs are unique within the schema, as required by the Schema constructor.
    int[] nextId = {columns.length};
    for (int i = 0; i < columns.length; i++) {
      Column col = columns[i];
      org.apache.iceberg.types.Type icebergType = toIcebergType(col.dataType(), nextId);
      if (col.nullable()) {
        fields.add(Types.NestedField.optional(i, col.name(), icebergType, col.comment()));
      } else {
        fields.add(Types.NestedField.required(i, col.name(), icebergType, col.comment()));
      }
    }
    return new Schema(fields);
  }

  private static org.apache.iceberg.types.Type toIcebergType(Type type) {
    int[] nextId = {0};
    return toIcebergType(type, nextId);
  }

  private static org.apache.iceberg.types.Type toIcebergType(Type type, int[] nextId) {
    if (type instanceof ListType) {
      ListType listType = (ListType) type;
      org.apache.iceberg.types.Type elementType = toIcebergType(listType.elementType(), nextId);
      int elementId = nextId[0]++;
      return listType.elementNullable()
          ? Types.ListType.ofOptional(elementId, elementType)
          : Types.ListType.ofRequired(elementId, elementType);
    }
    if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      org.apache.iceberg.types.Type keyType = toIcebergType(mapType.keyType(), nextId);
      org.apache.iceberg.types.Type valueType = toIcebergType(mapType.valueType(), nextId);
      int keyId = nextId[0]++;
      int valueId = nextId[0]++;
      return mapType.valueNullable()
          ? Types.MapType.ofOptional(keyId, valueId, keyType, valueType)
          : Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    }
    if (type instanceof StructType) {
      StructType structType = (StructType) type;
      List<Types.NestedField> nestedFields = new ArrayList<>();
      for (StructType.Field field : structType.fields()) {
        org.apache.iceberg.types.Type fieldType = toIcebergType(field.type(), nextId);
        int fieldId = nextId[0]++;
        nestedFields.add(
            field.nullable()
                ? Types.NestedField.optional(fieldId, field.name(), fieldType, field.comment())
                : Types.NestedField.required(fieldId, field.name(), fieldType, field.comment()));
      }
      return Types.StructType.of(nestedFields);
    }
    if (type instanceof BooleanType) {
      return Types.BooleanType.get();
    }
    if (type instanceof Type.IntegralType && !((Type.IntegralType) type).signed()) {
      throw new IllegalArgumentException(
          String.format(
              "Unsigned type %s is not supported by Iceberg; use a signed type",
              type.simpleString()));
    }
    if (type instanceof ByteType || type instanceof ShortType) {
      throw new IllegalArgumentException(
          String.format(
              "Type %s is not supported by Iceberg; use integer instead", type.simpleString()));
    }
    if (type instanceof IntegerType) {
      return Types.IntegerType.get();
    }
    if (type instanceof LongType) {
      return Types.LongType.get();
    }
    if (type instanceof FloatType) {
      return Types.FloatType.get();
    }
    if (type instanceof DoubleType) {
      return Types.DoubleType.get();
    }
    if (type instanceof StringType
        || type instanceof VarCharType
        || type instanceof FixedCharType) {
      return Types.StringType.get();
    }
    if (type instanceof DateType) {
      return Types.DateType.get();
    }
    if (type instanceof TimeType) {
      TimeType timeType = (TimeType) type;
      if (!timeType.hasPrecisionSet() || timeType.precision() == MICROSECOND_PRECISION) {
        return Types.TimeType.get();
      }
      throw new IllegalArgumentException(
          "Iceberg only supports microsecond precision (6) for time type, got: "
              + timeType.precision());
    }
    if (type instanceof TimestampType) {
      TimestampType tsType = (TimestampType) type;
      if (!tsType.hasPrecisionSet() || tsType.precision() == MICROSECOND_PRECISION) {
        return tsType.hasTimeZone()
            ? Types.TimestampType.withZone()
            : Types.TimestampType.withoutZone();
      }
      // Nanosecond timestamps need Iceberg format version 3, which Iceberg checks at create.
      if (tsType.precision() == NANOSECOND_PRECISION) {
        return tsType.hasTimeZone()
            ? Types.TimestampNanoType.withZone()
            : Types.TimestampNanoType.withoutZone();
      }
      throw new IllegalArgumentException(
          "Iceberg only supports microsecond (6) or nanosecond (9) precision for timestamp type,"
              + " got: "
              + tsType.precision());
    }
    if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      return Types.DecimalType.of(dt.precision(), dt.scale());
    }
    if (type instanceof BinaryType) {
      return Types.BinaryType.get();
    }
    if (type instanceof FixedType) {
      return Types.FixedType.ofLength(((FixedType) type).length());
    }
    if (type instanceof UUIDType) {
      return Types.UUIDType.get();
    }
    if (type instanceof VariantType) {
      return Types.VariantType.get();
    }
    if (type instanceof GeometryType) {
      return Types.GeometryType.of(((GeometryType) type).crs());
    }
    if (type instanceof GeographyType) {
      GeographyType geography = (GeographyType) type;
      return Types.GeographyType.of(geography.crs(), EdgeAlgorithm.fromName(geography.algorithm()));
    }
    if (type instanceof NullType) {
      return Types.UnknownType.get();
    }
    throw new IllegalArgumentException(
        String.format("Type %s is not supported by Iceberg tables", type.simpleString()));
  }

  // ---------------------------------------------------------------------------
  // Partition / sort conversion (Gravitino -> Iceberg)
  // ---------------------------------------------------------------------------

  @VisibleForTesting
  static PartitionSpec toPartitionSpec(Schema schema, @Nullable Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (Transform transform : partitions) {
      Preconditions.checkArgument(transform != null, "Partition transform cannot be null");
      // Iceberg checks the source type only when the spec is built, so each field is first built
      // alone to name the field that does not fit.
      try {
        addPartitionField(PartitionSpec.builderFor(schema), transform).build();
        addPartitionField(builder, transform);
      } catch (ValidationException | IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format("Invalid partition %s: %s", describe(transform), e.getMessage()), e);
      }
    }
    try {
      return builder.build();
    } catch (ValidationException | IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Invalid partitioning %s: %s", describe(partitions), e.getMessage()), e);
    }
  }

  private static PartitionSpec.Builder addPartitionField(
      PartitionSpec.Builder builder, Transform transform) {
    if (transform instanceof Transforms.IdentityTransform) {
      return builder.identity(
          String.join(DOT, ((Transforms.IdentityTransform) transform).fieldName()));
    } else if (transform instanceof Transforms.BucketTransform) {
      Transforms.BucketTransform bucket = (Transforms.BucketTransform) transform;
      String[][] fieldNames = bucket.fieldNames();
      Preconditions.checkArgument(
          fieldNames.length == 1, "Iceberg buckets take exactly one column");
      return builder.bucket(String.join(DOT, fieldNames[0]), bucket.numBuckets());
    } else if (transform instanceof Transforms.TruncateTransform) {
      Transforms.TruncateTransform truncate = (Transforms.TruncateTransform) transform;
      return builder.truncate(String.join(DOT, truncate.fieldName()), truncate.width());
    } else if (transform instanceof Transforms.YearTransform) {
      return builder.year(String.join(DOT, ((Transforms.YearTransform) transform).fieldName()));
    } else if (transform instanceof Transforms.MonthTransform) {
      return builder.month(String.join(DOT, ((Transforms.MonthTransform) transform).fieldName()));
    } else if (transform instanceof Transforms.DayTransform) {
      return builder.day(String.join(DOT, ((Transforms.DayTransform) transform).fieldName()));
    } else if (transform instanceof Transforms.HourTransform) {
      return builder.hour(String.join(DOT, ((Transforms.HourTransform) transform).fieldName()));
    }
    throw new IllegalArgumentException(
        "Iceberg supports identity, bucket, truncate, year, month, day and hour partitions");
  }

  @VisibleForTesting
  static org.apache.iceberg.SortOrder toSortOrder(Schema schema, @Nullable SortOrder[] sortOrders) {
    if (sortOrders == null || sortOrders.length == 0) {
      return org.apache.iceberg.SortOrder.unsorted();
    }
    org.apache.iceberg.SortOrder.Builder builder = org.apache.iceberg.SortOrder.builderFor(schema);
    for (SortOrder sortOrder : sortOrders) {
      Preconditions.checkArgument(sortOrder != null, "Sort order cannot be null");
      // The term is bound to the schema when the order is built, so each term is first built
      // alone to name the term that does not fit.
      try {
        addSortField(org.apache.iceberg.SortOrder.builderFor(schema), sortOrder).build();
        addSortField(builder, sortOrder);
      } catch (ValidationException | IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Invalid sort order %s: %s", describe(sortOrder.expression()), e.getMessage()),
            e);
      }
    }
    return builder.build();
  }

  private static org.apache.iceberg.SortOrder.Builder addSortField(
      org.apache.iceberg.SortOrder.Builder builder, SortOrder sortOrder) {
    Expression expression = sortOrder.expression();
    org.apache.iceberg.SortDirection direction = toIcebergDirection(sortOrder.direction());
    NullOrder nullOrder = toIcebergNullOrder(sortOrder.nullOrdering());
    if (expression instanceof NamedReference.FieldReference) {
      String fieldName = String.join(DOT, ((NamedReference.FieldReference) expression).fieldName());
      return builder.sortBy(fieldName, direction, nullOrder);
    } else if (expression instanceof FunctionExpression) {
      return builder.sortBy(
          toIcebergSortTerm((FunctionExpression) expression), direction, nullOrder);
    }
    throw new IllegalArgumentException(
        "Iceberg sorts by a column or by a bucket, truncate, year, month, day or hour of a column");
  }

  private static UnboundTerm<Object> toIcebergSortTerm(FunctionExpression func) {
    String name = func.functionName().toLowerCase(Locale.ROOT);
    switch (name) {
      case "bucket":
        Preconditions.checkArgument(
            func.arguments().length == 2, "bucket takes a bucket count and a column");
        int numBuckets = getIntLiteral(func.arguments()[0]);
        String bucketField = getFieldName(func.arguments()[1]);
        return Expressions.bucket(bucketField, numBuckets);
      case "truncate":
        Preconditions.checkArgument(
            func.arguments().length == 2, "truncate takes a width and a column");
        int width = getIntLiteral(func.arguments()[0]);
        String truncateField = getFieldName(func.arguments()[1]);
        return Expressions.truncate(truncateField, width);
      case "year":
        return Expressions.year(getSingleField(func));
      case "month":
        return Expressions.month(getSingleField(func));
      case "day":
        return Expressions.day(getSingleField(func));
      case "hour":
        return Expressions.hour(getSingleField(func));
      default:
        throw new IllegalArgumentException(
            "Iceberg sorts by a column or by a bucket, truncate, year, month, day or hour of a"
                + " column");
    }
  }

  private static String getSingleField(FunctionExpression func) {
    Preconditions.checkArgument(
        func.arguments().length == 1, "%s takes one column", func.functionName());
    return getFieldName(func.arguments()[0]);
  }

  private static String getFieldName(Expression expr) {
    Preconditions.checkArgument(
        expr instanceof NamedReference.FieldReference, "expected a column, got %s", describe(expr));
    return String.join(DOT, ((NamedReference.FieldReference) expr).fieldName());
  }

  private static int getIntLiteral(Expression expr) {
    Preconditions.checkArgument(
        expr instanceof Literal, "expected an integer, got %s", describe(expr));
    Object value = ((Literal<?>) expr).value();
    try {
      return Integer.parseInt(String.valueOf(value));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("expected an integer, got " + value, e);
    }
  }

  /** Renders partition transforms the way a user writes them, such as {@code bucket(4, id)}. */
  private static String describe(Transform[] transforms) {
    return Arrays.stream(transforms)
        .map(t -> t == null ? "null" : describe(t))
        .collect(Collectors.joining(", ", "[", "]"));
  }

  private static String describe(Transform transform) {
    return transform.name()
        + Arrays.stream(transform.arguments())
            .map(GlueIcebergTableHelper::describe)
            .collect(Collectors.joining(", ", "(", ")"));
  }

  /** Renders a column reference, literal or function the way a user writes it. */
  private static String describe(Expression expr) {
    if (expr instanceof NamedReference.FieldReference) {
      return String.join(DOT, ((NamedReference.FieldReference) expr).fieldName());
    }
    if (expr instanceof Literal) {
      return String.valueOf(((Literal<?>) expr).value());
    }
    if (expr instanceof FunctionExpression) {
      FunctionExpression func = (FunctionExpression) expr;
      return func.functionName()
          + Arrays.stream(func.arguments())
              .map(GlueIcebergTableHelper::describe)
              .collect(Collectors.joining(", ", "(", ")"));
    }
    return expr.getClass().getSimpleName();
  }

  private static org.apache.iceberg.SortDirection toIcebergDirection(SortDirection direction) {
    return direction == SortDirection.ASCENDING
        ? org.apache.iceberg.SortDirection.ASC
        : org.apache.iceberg.SortDirection.DESC;
  }

  private static NullOrder toIcebergNullOrder(NullOrdering ordering) {
    return ordering == NullOrdering.NULLS_FIRST ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
  }

  /**
   * Returns the column name for the given Iceberg field ID, throwing if the field is not found in
   * the schema.
   */
  private static String columnName(Schema schema, int sourceId) {
    String colName = schema.findColumnName(sourceId);
    Preconditions.checkState(
        colName != null,
        "No column found for Iceberg field ID %s; schema may be out of sync",
        sourceId);
    return colName;
  }
}
