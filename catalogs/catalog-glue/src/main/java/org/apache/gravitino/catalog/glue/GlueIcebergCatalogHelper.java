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
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.expressions.FunctionExpression;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.literals.Literal;
import org.apache.gravitino.rel.expressions.sorts.NullOrdering;
import org.apache.gravitino.rel.expressions.sorts.SortDirection;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper that delegates Iceberg table creation to the Iceberg SDK's {@code GlueCatalog}.
 *
 * <p>Unlike the native AWS Glue SDK {@code OpenTableFormatInput} approach, the Iceberg SDK writes
 * the {@code metadata.json} file to S3 and registers the table in Glue with the correct {@code
 * metadata_location} parameter, making the table usable by Trino Lakehouse connector and other
 * Iceberg-native query engines.
 */
final class GlueIcebergCatalogHelper {

  private static final Logger LOG = LoggerFactory.getLogger(GlueIcebergCatalogHelper.class);

  private static final String DOT = ".";

  private GlueIcebergCatalogHelper() {}

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
    icebergProps.put("warehouse", "/tmp/gravitino-glue-iceberg");
    icebergProps.put("catalog-impl", "GlueCatalog");
    icebergProps.put("client.region", region);

    String catalogId = config.get(GlueConstants.AWS_GLUE_CATALOG_ID);
    if (catalogId != null) {
      icebergProps.put("glue.id", catalogId);
    }

    String accessKey = config.get(GlueConstants.AWS_ACCESS_KEY_ID);
    String secretKey = config.get(GlueConstants.AWS_SECRET_ACCESS_KEY);
    if (accessKey != null && secretKey != null) {
      icebergProps.put("client.access-key-id", accessKey);
      icebergProps.put("client.secret-access-key", secretKey);
    }

    String endpoint = config.get(GlueConstants.AWS_GLUE_ENDPOINT);
    if (endpoint != null) {
      icebergProps.put("client.endpoint", endpoint);
      icebergProps.put("s3.endpoint", endpoint);
    }

    icebergProps.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");

    GlueCatalog glueCatalog = new GlueCatalog();
    glueCatalog.initialize("gravitino-glue-iceberg", icebergProps);
    LOG.info("Initialized Iceberg GlueCatalog for region {}", region);
    return glueCatalog;
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
      String comment,
      Map<String, String> properties,
      Transform[] partitions,
      SortOrder[] sortOrders) {

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

    TableIdentifier tableId = TableIdentifier.of(dbName, tableName);

    LOG.info("Creating Iceberg table {} at location {} via Iceberg SDK", tableId, location);

    icebergCatalog
        .buildTable(tableId, schema)
        .withLocation(location)
        .withPartitionSpec(spec)
        .withSortOrder(icebergSortOrder)
        .withProperties(tableProps)
        .create();
  }

  // ---------------------------------------------------------------------------
  // Type conversion
  // ---------------------------------------------------------------------------

  private static Schema toIcebergSchema(Column[] columns) {
    java.util.List<org.apache.iceberg.types.Types.NestedField> fields = new java.util.ArrayList<>();
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
  // Partition conversion
  // ---------------------------------------------------------------------------

  private static PartitionSpec toPartitionSpec(Schema schema, Transform[] partitions) {
    if (partitions == null || partitions.length == 0) {
      return PartitionSpec.unpartitioned();
    }
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (Transform transform : partitions) {
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

  // ---------------------------------------------------------------------------
  // Sort order conversion
  // ---------------------------------------------------------------------------

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
    return Integer.parseInt(String.valueOf(value));
  }

  private static org.apache.iceberg.SortDirection toIcebergDirection(SortDirection direction) {
    return direction == SortDirection.ASCENDING
        ? org.apache.iceberg.SortDirection.ASC
        : org.apache.iceberg.SortDirection.DESC;
  }

  private static NullOrder toIcebergNullOrder(NullOrdering ordering) {
    return ordering == NullOrdering.NULLS_FIRST ? NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
  }
}
