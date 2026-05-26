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
import static org.apache.gravitino.rel.types.Types.IntegerType;
import static org.apache.gravitino.rel.types.Types.ListType;
import static org.apache.gravitino.rel.types.Types.LongType;
import static org.apache.gravitino.rel.types.Types.MapType;
import static org.apache.gravitino.rel.types.Types.ShortType;
import static org.apache.gravitino.rel.types.Types.StringType;
import static org.apache.gravitino.rel.types.Types.StructType;
import static org.apache.gravitino.rel.types.Types.TimeType;
import static org.apache.gravitino.rel.types.Types.TimestampType;
import static org.apache.gravitino.rel.types.Types.UUIDType;
import static org.apache.gravitino.rel.types.Types.VarCharType;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
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
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
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

  private static final Logger LOG = LoggerFactory.getLogger(GlueIcebergTableHelper.class);

  private static final String DOT = ".";

  // Iceberg GlueCatalog properties (not defined in IcebergConstants).
  private static final String CATALOG_IMPL = "catalog-impl";
  private static final String GLUE_CATALOG = "GlueCatalog";
  private static final String GLUE_ID = "glue.id";
  private static final String GLUE_ENDPOINT = "glue.endpoint";
  private static final String CLIENT_CREDENTIALS_PROVIDER = "client.credentials-provider";
  private static final String CLIENT_CREDENTIALS_PROVIDER_ACCESS_KEY_ID =
      "client.credentials-provider.access-key-id";
  private static final String CLIENT_CREDENTIALS_PROVIDER_SECRET_ACCESS_KEY =
      "client.credentials-provider.secret-access-key";

  private static final Set<String> EXCLUDED_TABLE_PROPS =
      Set.of(
          GlueConstants.LOCATION,
          GlueConstants.METADATA_LOCATION,
          GlueConstants.INPUT_FORMAT_CLASS,
          GlueConstants.OUTPUT_FORMAT,
          GlueConstants.SERDE_LIB);

  // Iceberg 1.10+ uses "bucket[n]" / "truncate[w]" format for Transform.toString().
  private static final Pattern TRANSFORM_PARAM_PATTERN = Pattern.compile(".*\\[(\\d+)\\]");

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
    if (catalogId != null) {
      icebergProps.put(GLUE_ID, catalogId);
    }

    String accessKey = config.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretKey = config.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    if (accessKey != null && secretKey != null) {
      icebergProps.put(
          CLIENT_CREDENTIALS_PROVIDER, GravitinoGlueCredentialsProvider.class.getName());
      icebergProps.put(CLIENT_CREDENTIALS_PROVIDER_ACCESS_KEY_ID, accessKey);
      icebergProps.put(CLIENT_CREDENTIALS_PROVIDER_SECRET_ACCESS_KEY, secretKey);
    }

    String endpoint = config.get(GlueConstants.AWS_GLUE_ENDPOINT);
    if (StringUtils.isNotBlank(endpoint)) {
      icebergProps.put(GLUE_ENDPOINT, endpoint);
    }

    if (accessKey != null && secretKey != null) {
      icebergProps.put(IcebergConstants.ICEBERG_S3_ACCESS_KEY_ID, accessKey);
      icebergProps.put(IcebergConstants.ICEBERG_S3_SECRET_ACCESS_KEY, secretKey);
    }

    icebergProps.put(IcebergConstants.IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");

    GlueCatalog glueCatalog = new GlueCatalog();
    glueCatalog.initialize("gravitino-glue-iceberg", icebergProps);
    LOG.info("Initialized Iceberg GlueCatalog for region {}", region);
    return glueCatalog;
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
    // Iceberg properties take precedence over Glue parameters for overlapping keys.
    Map<String, String> mergedProps = new HashMap<>(table.properties());
    mergedProps.putAll(icebergTable.properties());
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
   * <p>TIME and TIMESTAMP types are always returned with microsecond (6) precision, matching
   * Iceberg's internal representation.
   */
  // TODO: the Iceberg-to-Gravitino conversions in this class (type mapping, partition spec,
  // sort order, etc.) duplicate logic in catalog-lakehouse-iceberg. Consider extracting them
  // to a shared layer (e.g. catalog-common) so both catalogs can reuse the code.
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
        return TimeType.of(6);
      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) icebergType;
        return ts.shouldAdjustToUTC()
            ? TimestampType.withTimeZone(6)
            : TimestampType.withoutTimeZone(6);
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

    String format = properties.get(GlueConstants.FORMAT);
    if (format != null) {
      tableProps.put("write.format.default", format.toLowerCase(Locale.ROOT));
    }

    String tableFormat = properties.get(GlueConstants.TABLE_FORMAT);
    if (tableFormat != null) {
      tableProps.put(GlueConstants.TABLE_FORMAT, tableFormat);
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
   * <p>Delegates schema changes to {@link UpdateSchema} and property changes to {@link
   * UpdateProperties}.
   *
   * <p><b>Note:</b> Schema changes and property changes are committed in two separate transactions.
   * If the schema commit succeeds but the property commit fails, the table is left in a partially
   * altered state. This is a known limitation of the current Iceberg SDK integration.
   *
   * @param icebergCatalog the Iceberg Glue catalog
   * @param dbName the Glue database name
   * @param tableName the table name
   * @param changes the table changes to apply
   */
  static void alterTable(
      Catalog icebergCatalog, String dbName, String tableName, TableChange... changes) {
    Table table = icebergCatalog.loadTable(TableIdentifier.of(dbName, tableName));

    boolean hasSchemaChange = false;
    boolean hasPropChange = false;
    for (TableChange change : changes) {
      if (change instanceof TableChange.ColumnChange) {
        hasSchemaChange = true;
      } else if (change instanceof TableChange.SetProperty
          || change instanceof TableChange.RemoveProperty) {
        hasPropChange = true;
      } else {
        throw new IllegalArgumentException(
            "Unsupported table change for Iceberg table: " + change.getClass().getSimpleName());
      }
    }

    if (hasSchemaChange) {
      UpdateSchema update = table.updateSchema();
      for (TableChange change : changes) {
        if (change instanceof TableChange.AddColumn) {
          TableChange.AddColumn add = (TableChange.AddColumn) change;
          Preconditions.checkArgument(
              add.fieldName().length == 1, "Nested column additions are not supported");
          update.addColumn(add.fieldName()[0], toIcebergType(add.getDataType()), add.getComment());
          if (!add.isNullable()) {
            update.requireColumn(add.fieldName()[0]);
          }
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
          update.updateColumn(
              upd.fieldName()[0],
              (org.apache.iceberg.types.Type.PrimitiveType) toIcebergType(upd.getNewDataType()));
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
      LOG.info("Altered Iceberg table {}.{} schema via Iceberg SDK", dbName, tableName);
    }

    if (hasPropChange) {
      UpdateProperties update = table.updateProperties();
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
      LOG.info("Altered Iceberg table {}.{} properties via Iceberg SDK", dbName, tableName);
    }
  }

  // ---------------------------------------------------------------------------
  // Partition / sort conversion (Iceberg -> Gravitino)
  // ---------------------------------------------------------------------------

  /**
   * Converts an Iceberg {@link org.apache.iceberg.PartitionSpec} to Gravitino {@link Transform}s.
   *
   * <p>Supports identity, year, month, day, hour, bucket, and truncate transforms.
   */
  static Transform[] convertPartitionSpec(PartitionSpec spec, Schema schema) {
    return spec.fields().stream()
        .map(
            field -> {
              String colName = resolveColumnName(schema, field.sourceId());
              String transformStr = field.transform().toString().toLowerCase(Locale.ROOT);
              if (transformStr.startsWith("identity")) {
                return Transforms.identity(colName);
              } else if (transformStr.startsWith("year")) {
                return Transforms.year(colName);
              } else if (transformStr.startsWith("month")) {
                return Transforms.month(colName);
              } else if (transformStr.startsWith("day")) {
                return Transforms.day(colName);
              } else if (transformStr.startsWith("hour")) {
                return Transforms.hour(colName);
              } else if (transformStr.startsWith("bucket")) {
                int numBuckets = extractTransformParam(field.transform().toString());
                return Transforms.bucket(numBuckets, new String[] {colName});
              } else if (transformStr.startsWith("truncate")) {
                int width = extractTransformParam(field.transform().toString());
                return Transforms.truncate(width, new String[] {colName});
              } else {
                throw new IllegalArgumentException(
                    "Unsupported partition transform: " + transformStr);
              }
            })
        .toArray(Transform[]::new);
  }

  /** Converts an Iceberg {@link org.apache.iceberg.SortOrder} to Gravitino {@link SortOrder}s. */
  static SortOrder[] convertIcebergSortOrder(
      org.apache.iceberg.SortOrder iceSortOrder, Schema schema) {
    if (iceSortOrder == null || iceSortOrder.fields().isEmpty()) {
      return new SortOrder[0];
    }
    return iceSortOrder.fields().stream()
        .map(
            field -> {
              String colName = resolveColumnName(schema, field.sourceId());
              SortDirection direction =
                  field.direction() == org.apache.iceberg.SortDirection.ASC
                      ? SortDirection.ASCENDING
                      : SortDirection.DESCENDING;
              NullOrdering nullOrdering =
                  field.nullOrder() == org.apache.iceberg.NullOrder.NULLS_FIRST
                      ? NullOrdering.NULLS_FIRST
                      : NullOrdering.NULLS_LAST;

              org.apache.iceberg.transforms.Transform<?, ?> transform = field.transform();
              String transformStr = transform.toString().toLowerCase(Locale.ROOT);
              if (transformStr.startsWith("identity")) {
                return SortOrders.of(NamedReference.field(colName), direction, nullOrdering);
              }

              Expression expr;
              if (transformStr.startsWith("year")) {
                expr = FunctionExpression.of("year", NamedReference.field(colName));
              } else if (transformStr.startsWith("month")) {
                expr = FunctionExpression.of("month", NamedReference.field(colName));
              } else if (transformStr.startsWith("day")) {
                expr = FunctionExpression.of("day", NamedReference.field(colName));
              } else if (transformStr.startsWith("hour")) {
                expr = FunctionExpression.of("hour", NamedReference.field(colName));
              } else if (transformStr.startsWith("bucket")) {
                int numBuckets = extractTransformParam(transformStr);
                expr =
                    FunctionExpression.of(
                        "bucket",
                        Literals.integerLiteral(numBuckets),
                        NamedReference.field(colName));
              } else if (transformStr.startsWith("truncate")) {
                int width = extractTransformParam(transformStr);
                expr =
                    FunctionExpression.of(
                        "truncate", Literals.integerLiteral(width), NamedReference.field(colName));
              } else {
                expr = NamedReference.field(colName);
              }
              return SortOrders.of(expr, direction, nullOrdering);
            })
        .toArray(SortOrder[]::new);
  }

  // ---------------------------------------------------------------------------
  // Type conversion (Gravitino -> Iceberg)
  // ---------------------------------------------------------------------------

  private static Schema toIcebergSchema(Column[] columns) {
    List<Types.NestedField> fields = new ArrayList<>();
    // Field IDs are assigned sequentially starting from 0. This is the Iceberg convention
    // for initial schema creation. Schema evolution reuses existing IDs from the table.
    for (int i = 0; i < columns.length; i++) {
      Column col = columns[i];
      org.apache.iceberg.types.Type icebergType = toIcebergType(col.dataType());
      if (col.nullable()) {
        fields.add(Types.NestedField.optional(i, col.name(), icebergType, col.comment()));
      } else {
        fields.add(Types.NestedField.required(i, col.name(), icebergType, col.comment()));
      }
    }
    return new Schema(fields);
  }

  private static org.apache.iceberg.types.Type toIcebergType(Type type) {
    if (type instanceof BooleanType) {
      return Types.BooleanType.get();
    }
    if (type instanceof ByteType || type instanceof ShortType || type instanceof IntegerType) {
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
      if (!timeType.hasPrecisionSet() || timeType.precision() == 6) {
        return Types.TimeType.get();
      }
      throw new IllegalArgumentException(
          "Iceberg only supports microsecond precision (6) for time type, got: "
              + timeType.precision());
    }
    if (type instanceof TimestampType) {
      TimestampType tsType = (TimestampType) type;
      if (!tsType.hasPrecisionSet() || tsType.precision() == 6) {
        return tsType.hasTimeZone()
            ? Types.TimestampType.withZone()
            : Types.TimestampType.withoutZone();
      }
      throw new IllegalArgumentException(
          "Iceberg only supports microsecond precision (6) for timestamp type, got: "
              + tsType.precision());
    }
    if (type instanceof DecimalType) {
      DecimalType dt = (DecimalType) type;
      return Types.DecimalType.of(dt.precision(), dt.scale());
    }
    if (type instanceof BinaryType || type instanceof FixedType) {
      return Types.BinaryType.get();
    }
    if (type instanceof UUIDType) {
      return Types.UUIDType.get();
    }
    throw new UnsupportedOperationException("Unsupported type for Iceberg: " + type.simpleString());
  }

  // ---------------------------------------------------------------------------
  // Partition / sort conversion (Gravitino -> Iceberg)
  // ---------------------------------------------------------------------------

  private static PartitionSpec toPartitionSpec(Schema schema, Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (Transform transform : partitions) {
      Preconditions.checkArgument(transform != null, "Partition transform cannot be null");
      if (transform instanceof Transforms.IdentityTransform) {
        String colName = String.join(DOT, ((Transforms.IdentityTransform) transform).fieldName());
        builder.identity(colName);
      } else if (transform instanceof Transforms.BucketTransform) {
        Transforms.BucketTransform bucket = (Transforms.BucketTransform) transform;
        String[][] fieldNames = bucket.fieldNames();
        Preconditions.checkArgument(
            fieldNames.length == 1,
            "Iceberg partition does not support multi-field bucket: %s",
            transform);
        builder.bucket(String.join(DOT, fieldNames[0]), bucket.numBuckets());
      } else if (transform instanceof Transforms.TruncateTransform) {
        Transforms.TruncateTransform truncate = (Transforms.TruncateTransform) transform;
        builder.truncate(String.join(DOT, truncate.fieldName()), truncate.width());
      } else if (transform instanceof Transforms.YearTransform) {
        builder.year(String.join(DOT, ((Transforms.YearTransform) transform).fieldName()));
      } else if (transform instanceof Transforms.MonthTransform) {
        builder.month(String.join(DOT, ((Transforms.MonthTransform) transform).fieldName()));
      } else if (transform instanceof Transforms.DayTransform) {
        builder.day(String.join(DOT, ((Transforms.DayTransform) transform).fieldName()));
      } else if (transform instanceof Transforms.HourTransform) {
        builder.hour(String.join(DOT, ((Transforms.HourTransform) transform).fieldName()));
      } else {
        throw new UnsupportedOperationException("Transform is not supported: " + transform.name());
      }
    }
    return builder.build();
  }

  private static org.apache.iceberg.SortOrder toSortOrder(Schema schema, SortOrder[] sortOrders) {
    if (sortOrders == null || sortOrders.length == 0) {
      return org.apache.iceberg.SortOrder.unsorted();
    }
    org.apache.iceberg.SortOrder.Builder builder = org.apache.iceberg.SortOrder.builderFor(schema);
    for (SortOrder sortOrder : sortOrders) {
      if (sortOrder.expression() instanceof NamedReference.FieldReference) {
        String fieldName =
            String.join(DOT, ((NamedReference.FieldReference) sortOrder.expression()).fieldName());
        builder.sortBy(
            fieldName,
            toIcebergDirection(sortOrder.direction()),
            toIcebergNullOrder(sortOrder.nullOrdering()));
      } else if (sortOrder.expression() instanceof FunctionExpression) {
        FunctionExpression func = (FunctionExpression) sortOrder.expression();
        UnboundTerm<Object> term = toIcebergSortTerm(func);
        builder.sortBy(
            term,
            toIcebergDirection(sortOrder.direction()),
            toIcebergNullOrder(sortOrder.nullOrdering()));
      } else {
        throw new UnsupportedOperationException(
            "Sort expression is not supported: " + sortOrder.expression());
      }
    }
    return builder.build();
  }

  private static UnboundTerm<Object> toIcebergSortTerm(FunctionExpression func) {
    String name = func.functionName().toLowerCase(Locale.ROOT);
    switch (name) {
      case "bucket":
        Preconditions.checkArgument(
            func.arguments().length == 2, "Bucket sort must have 2 arguments");
        int numBuckets = getIntLiteral(func.arguments()[0]);
        String bucketField = getFieldName(func.arguments()[1]);
        return Expressions.bucket(bucketField, numBuckets);
      case "truncate":
        Preconditions.checkArgument(
            func.arguments().length == 2, "Truncate sort must have 2 arguments");
        int width = getIntLiteral(func.arguments()[0]);
        String truncateField = getFieldName(func.arguments()[1]);
        return Expressions.truncate(truncateField, width);
      case "year":
        return Expressions.year(getSingleField(func.arguments()));
      case "month":
        return Expressions.month(getSingleField(func.arguments()));
      case "day":
        return Expressions.day(getSingleField(func.arguments()));
      case "hour":
        return Expressions.hour(getSingleField(func.arguments()));
      default:
        throw new UnsupportedOperationException(
            "Sort function not supported: " + func.functionName());
    }
  }

  private static String getSingleField(Expression[] args) {
    Preconditions.checkArgument(args.length == 1, "Sort function must have 1 argument");
    return getFieldName(args[0]);
  }

  private static String getFieldName(Expression expr) {
    Preconditions.checkArgument(
        expr instanceof NamedReference.FieldReference,
        "Expected field reference, got: %s",
        expr.getClass().getSimpleName());
    return String.join(DOT, ((NamedReference.FieldReference) expr).fieldName());
  }

  private static int getIntLiteral(Expression expr) {
    Preconditions.checkArgument(
        expr instanceof Literal, "Expected literal, got: %s", expr.getClass().getSimpleName());
    Object value = ((Literal<?>) expr).value();
    try {
      return Integer.parseInt(String.valueOf(value));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Expected integer literal, got: " + value + " in expression: " + expr, e);
    }
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
  private static String resolveColumnName(Schema schema, int sourceId) {
    String colName = schema.findColumnName(sourceId);
    Preconditions.checkState(
        colName != null,
        "No column found for Iceberg field ID %s; schema may be out of sync",
        sourceId);
    return colName;
  }

  /**
   * Extracts an integer parameter from an Iceberg transform string (e.g. "bucket[16]" or
   * "truncate[8]").
   *
   * <p>Iceberg does not expose transform parameters through a typed API, so string parsing is used.
   */
  private static int extractTransformParam(String transformStr) {
    String param = TRANSFORM_PARAM_PATTERN.matcher(transformStr).replaceAll("$1");
    if (param.equals(transformStr)) {
      throw new IllegalArgumentException(
          "Cannot extract numeric parameter from transform: " + transformStr);
    }
    try {
      return Integer.parseInt(param);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid numeric parameter in transform: " + transformStr, e);
    }
  }
}
