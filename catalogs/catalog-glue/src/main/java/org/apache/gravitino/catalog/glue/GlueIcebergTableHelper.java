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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.catalog.lakehouse.iceberg.IcebergConstants;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.TableChange;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
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
   * @param config Gravitino catalog configuration (region, credentials, endpoint, etc.)
   * @return an initialized Iceberg Glue catalog
   */
  static Catalog createGlueCatalog(Map<String, String> config) {
    String region = config.get(GlueConstants.AWS_REGION);
    Preconditions.checkArgument(region != null, "AWS region is required for Iceberg Glue catalog");

    Map<String, String> icebergProps = new HashMap<>();
    // Warehouse is required by Iceberg catalog initialization but is not used when each table
    // provides an explicit location.
    icebergProps.put(IcebergConstants.WAREHOUSE, "/tmp/gravitino-glue-iceberg");
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
    if (endpoint != null) {
      icebergProps.put(GLUE_ENDPOINT, endpoint);
      icebergProps.put(IcebergConstants.ICEBERG_S3_ENDPOINT, endpoint);
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

    if (!icebergTable.spec().fields().isEmpty()) {
      table.setPartitioning(convertPartitionSpec(icebergTable.spec(), icebergTable.schema()));
    }
    if (!icebergTable.sortOrder().fields().isEmpty()) {
      table.setSortOrders(convertIcebergSortOrder(icebergTable.sortOrder(), icebergTable.schema()));
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
    } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
      throw new org.apache.gravitino.exceptions.TableAlreadyExistsException(
          e, "Table %s.%s already exists", dbName, tableName);
    } catch (org.apache.iceberg.exceptions.ValidationException e) {
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
  static Transform[] convertPartitionSpec(
      org.apache.iceberg.PartitionSpec spec, org.apache.iceberg.Schema schema) {
    return spec.fields().stream()
        .map(
            field -> {
              String colName = schema.findColumnName(field.sourceId());
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
      org.apache.iceberg.SortOrder iceSortOrder, org.apache.iceberg.Schema schema) {
    if (iceSortOrder == null || iceSortOrder.fields().isEmpty()) {
      return new SortOrder[0];
    }
    return iceSortOrder.fields().stream()
        .map(
            field -> {
              String colName = schema.findColumnName(field.sourceId());
              SortDirection direction =
                  field.direction() == org.apache.iceberg.SortDirection.ASC
                      ? SortDirection.ASCENDING
                      : SortDirection.DESCENDING;
              NullOrdering nullOrdering =
                  field.nullOrder() == org.apache.iceberg.NullOrder.NULLS_FIRST
                      ? NullOrdering.NULLS_FIRST
                      : NullOrdering.NULLS_LAST;
              return SortOrders.of(NamedReference.field(colName), direction, nullOrdering);
            })
        .toArray(SortOrder[]::new);
  }

  // ---------------------------------------------------------------------------
  // Type conversion (Gravitino -> Iceberg)
  // ---------------------------------------------------------------------------

  private static Schema toIcebergSchema(Column[] columns) {
    List<org.apache.iceberg.types.Types.NestedField> fields = new java.util.ArrayList<>();
    // Field IDs are assigned sequentially starting from 0. This is the Iceberg convention
    // for initial schema creation. Schema evolution reuses existing IDs from the table.
    for (int i = 0; i < columns.length; i++) {
      Column col = columns[i];
      org.apache.iceberg.types.Type icebergType = toIcebergType(col.dataType());
      if (col.nullable()) {
        fields.add(
            org.apache.iceberg.types.Types.NestedField.optional(
                i, col.name(), icebergType, col.comment()));
      } else {
        fields.add(
            org.apache.iceberg.types.Types.NestedField.required(
                i, col.name(), icebergType, col.comment()));
      }
    }
    return new Schema(fields);
  }

  private static org.apache.iceberg.types.Type toIcebergType(Type type) {
    if (type instanceof Types.BooleanType) {
      return org.apache.iceberg.types.Types.BooleanType.get();
    }
    if (type instanceof Types.ByteType
        || type instanceof Types.ShortType
        || type instanceof Types.IntegerType) {
      return org.apache.iceberg.types.Types.IntegerType.get();
    }
    if (type instanceof Types.LongType) {
      return org.apache.iceberg.types.Types.LongType.get();
    }
    if (type instanceof Types.FloatType) {
      return org.apache.iceberg.types.Types.FloatType.get();
    }
    if (type instanceof Types.DoubleType) {
      return org.apache.iceberg.types.Types.DoubleType.get();
    }
    if (type instanceof Types.StringType
        || type instanceof Types.VarCharType
        || type instanceof Types.FixedCharType) {
      return org.apache.iceberg.types.Types.StringType.get();
    }
    if (type instanceof Types.DateType) {
      return org.apache.iceberg.types.Types.DateType.get();
    }
    if (type instanceof Types.TimeType) {
      Types.TimeType timeType = (Types.TimeType) type;
      if (!timeType.hasPrecisionSet() || timeType.precision() == 6) {
        return org.apache.iceberg.types.Types.TimeType.get();
      }
      throw new IllegalArgumentException(
          "Iceberg only supports microsecond precision (6) for time type, got: "
              + timeType.precision());
    }
    if (type instanceof Types.TimestampType) {
      Types.TimestampType tsType = (Types.TimestampType) type;
      if (!tsType.hasPrecisionSet() || tsType.precision() == 6) {
        return tsType.hasTimeZone()
            ? org.apache.iceberg.types.Types.TimestampType.withZone()
            : org.apache.iceberg.types.Types.TimestampType.withoutZone();
      }
      throw new IllegalArgumentException(
          "Iceberg only supports microsecond precision (6) for timestamp type, got: "
              + tsType.precision());
    }
    if (type instanceof Types.DecimalType) {
      Types.DecimalType dt = (Types.DecimalType) type;
      return org.apache.iceberg.types.Types.DecimalType.of(dt.precision(), dt.scale());
    }
    if (type instanceof Types.BinaryType || type instanceof Types.FixedType) {
      return org.apache.iceberg.types.Types.BinaryType.get();
    }
    if (type instanceof Types.UUIDType) {
      return org.apache.iceberg.types.Types.UUIDType.get();
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

  private static String getSingleField(org.apache.gravitino.rel.expressions.Expression[] args) {
    Preconditions.checkArgument(args.length == 1, "Sort function must have 1 argument");
    return getFieldName(args[0]);
  }

  private static String getFieldName(org.apache.gravitino.rel.expressions.Expression expr) {
    Preconditions.checkArgument(
        expr instanceof NamedReference.FieldReference,
        "Expected field reference, got: %s",
        expr.getClass().getSimpleName());
    return String.join(DOT, ((NamedReference.FieldReference) expr).fieldName());
  }

  private static int getIntLiteral(org.apache.gravitino.rel.expressions.Expression expr) {
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
   * Extracts an integer parameter from an Iceberg transform string (e.g. "bucket[16]" or
   * "truncate[8]").
   *
   * <p>TODO: Replace with typed Iceberg Transform API when available.
   */
  private static int extractTransformParam(String transformStr) {
    String param = transformStr.replaceAll(".*\\[(\\d+)\\]", "$1");
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
