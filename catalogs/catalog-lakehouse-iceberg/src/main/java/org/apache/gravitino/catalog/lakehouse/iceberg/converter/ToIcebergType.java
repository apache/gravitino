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
package org.apache.gravitino.catalog.lakehouse.iceberg.converter;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Convert Apache Gravitino types to Apache Iceberg types.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/SparkTypeToType.java
 */
public class ToIcebergType extends ToIcebergTypeVisitor<Type> {
  private final org.apache.gravitino.rel.types.Types.StructType root;
  private int nextId = 0;

  public ToIcebergType() {
    this.root = null;
  }

  public ToIcebergType(org.apache.gravitino.rel.types.Types.StructType root) {
    this.root = root;
    // the root struct's fields use the first ids
    this.nextId = root.fields().length;
  }

  private int getNextId() {
    return nextId++;
  }

  @SuppressWarnings("ReferenceEquality")
  @Override
  public Type struct(org.apache.gravitino.rel.types.Types.StructType struct, List<Type> types) {
    org.apache.gravitino.rel.types.Types.StructType.Field[] fields = struct.fields();
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.length);
    // Comparing the root node by reference equality.
    boolean isRoot = root == struct;
    for (int i = 0; i < fields.length; i += 1) {
      org.apache.gravitino.rel.types.Types.StructType.Field field = fields[i];
      Type type = types.get(i);

      int id;
      if (isRoot) {
        // for new conversions, use ordinals for ids in the root struct
        id = i;
      } else {
        id = getNextId();
      }

      String doc = field.comment();

      if (field.nullable()) {
        newFields.add(Types.NestedField.optional(id, field.name(), type, doc));
      } else {
        newFields.add(Types.NestedField.required(id, field.name(), type, doc));
      }
    }
    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(org.apache.gravitino.rel.types.Types.StructType.Field field, Type typeResult) {
    return typeResult;
  }

  @Override
  public Type array(org.apache.gravitino.rel.types.Types.ListType array, Type elementType) {
    if (array.elementNullable()) {
      return Types.ListType.ofOptional(getNextId(), elementType);
    } else {
      return Types.ListType.ofRequired(getNextId(), elementType);
    }
  }

  @Override
  public Type map(org.apache.gravitino.rel.types.Types.MapType map, Type keyType, Type valueType) {
    if (map.valueNullable()) {
      return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
    } else {
      return Types.MapType.ofRequired(getNextId(), getNextId(), keyType, valueType);
    }
  }

  @Override
  public Type atomic(org.apache.gravitino.rel.types.Type.PrimitiveType primitive) {
    if (primitive instanceof org.apache.gravitino.rel.types.Types.BooleanType) {
      return Types.BooleanType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.ByteType
        || primitive instanceof org.apache.gravitino.rel.types.Types.ShortType) {
      throw new IllegalArgumentException(
          "Iceberg do not support Byte and Short Type, use Integer instead");
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.IntegerType) {
      return Types.IntegerType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.LongType) {
      return Types.LongType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.FloatType) {
      return Types.FloatType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.DoubleType) {
      return Types.DoubleType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.StringType) {
      return Types.StringType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.DateType) {
      return Types.DateType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.TimeType) {
      org.apache.gravitino.rel.types.Types.TimeType timeType =
          (org.apache.gravitino.rel.types.Types.TimeType) primitive;
      // Iceberg only supports microsecond precision (6) for time type up to version 1.9.x
      // See: https://iceberg.apache.org/docs/1.9.1/schemas/
      if (!timeType.hasPrecisionSet() || timeType.precision() == 6) {
        return Types.TimeType.get();
      } else {
        throw new IllegalArgumentException(
            "Iceberg only supports microsecond precision (6) for time type, but got precision: "
                + timeType.precision());
      }
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.TimestampType) {
      org.apache.gravitino.rel.types.Types.TimestampType timestampType =
          (org.apache.gravitino.rel.types.Types.TimestampType) primitive;
      // Iceberg only supports microsecond precision (6) for timestamp/timestamptz types up to
      // version 1.9.x
      // See: https://iceberg.apache.org/docs/1.9.1/schemas/
      if (!timestampType.hasPrecisionSet() || timestampType.precision() == 6) {
        if (timestampType.hasTimeZone()) {
          return Types.TimestampType.withZone();
        } else {
          return Types.TimestampType.withoutZone();
        }
      } else {
        String timestampTypeName = timestampType.hasTimeZone() ? "timestamptz" : "timestamp";
        throw new IllegalArgumentException(
            "Iceberg only supports microsecond precision (6) for "
                + timestampTypeName
                + " type, but got precision: "
                + timestampType.precision());
      }
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.DecimalType) {
      return Types.DecimalType.of(
          ((org.apache.gravitino.rel.types.Types.DecimalType) primitive).precision(),
          ((org.apache.gravitino.rel.types.Types.DecimalType) primitive).scale());
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.FixedType) {
      return Types.FixedType.ofLength(
          ((org.apache.gravitino.rel.types.Types.FixedType) primitive).length());
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.BinaryType) {
      return Types.BinaryType.get();
    } else if (primitive instanceof org.apache.gravitino.rel.types.Types.UUIDType) {
      return Types.UUIDType.get();
    }
    throw new UnsupportedOperationException("Not a supported type: " + primitive.toString());
  }
}
