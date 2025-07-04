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

import java.util.ArrayList;
import java.util.List;
import org.apache.gravitino.rel.types.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * Implement a type converter to convert types in Apache Iceberg.
 *
 * <p>Referred from core/src/main/java/org/apache/iceberg/spark/TypeToSparkType.java
 */
public class FromIcebergType extends TypeUtil.SchemaVisitor<Type> {

  public static final int PRECISION_MICROSECOND = 6;

  public FromIcebergType() {}

  @Override
  public Type schema(Schema schema, Type structType) {
    throw new UnsupportedOperationException("Data conversion of schema type is not supported");
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> fieldResults) {
    List<org.apache.gravitino.rel.types.Types.StructType.Field> fieldsList = new ArrayList<>();
    List<Types.NestedField> originalFields = struct.fields();

    for (int i = 0; i < originalFields.size(); i++) {
      Types.NestedField nestedField = originalFields.get(i);
      fieldsList.add(
          org.apache.gravitino.rel.types.Types.StructType.Field.of(
              nestedField.name(),
              fieldResults.get(i),
              nestedField.isOptional(),
              nestedField.doc()));
    }
    return org.apache.gravitino.rel.types.Types.StructType.of(
        fieldsList.toArray(new org.apache.gravitino.rel.types.Types.StructType.Field[0]));
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    return fieldResult;
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    return org.apache.gravitino.rel.types.Types.ListType.of(
        elementResult, list.isElementOptional());
  }

  @Override
  public Type map(Types.MapType map, Type keyResult, Type valueResult) {
    return org.apache.gravitino.rel.types.Types.MapType.of(
        keyResult, valueResult, map.isValueOptional());
  }

  @Override
  public Type primitive(org.apache.iceberg.types.Type.PrimitiveType primitive) {
    switch (primitive.typeId()) {
      case BOOLEAN:
        return org.apache.gravitino.rel.types.Types.BooleanType.get();
      case INTEGER:
        return org.apache.gravitino.rel.types.Types.IntegerType.get();
      case LONG:
        return org.apache.gravitino.rel.types.Types.LongType.get();
      case FLOAT:
        return org.apache.gravitino.rel.types.Types.FloatType.get();
      case DOUBLE:
        return org.apache.gravitino.rel.types.Types.DoubleType.get();
      case DATE:
        return org.apache.gravitino.rel.types.Types.DateType.get();
      case TIME:
        return org.apache.gravitino.rel.types.Types.TimeType.of(PRECISION_MICROSECOND);
      case TIMESTAMP:
        Types.TimestampType ts = (Types.TimestampType) primitive;
        if (ts.shouldAdjustToUTC()) {
          return org.apache.gravitino.rel.types.Types.TimestampType.withTimeZone(
              PRECISION_MICROSECOND);
        } else {
          return org.apache.gravitino.rel.types.Types.TimestampType.withoutTimeZone(
              PRECISION_MICROSECOND);
        }
      case STRING:
        return org.apache.gravitino.rel.types.Types.StringType.get();
      case UUID:
        return org.apache.gravitino.rel.types.Types.UUIDType.get();
      case FIXED:
        Types.FixedType fixedType = (Types.FixedType) primitive;
        return org.apache.gravitino.rel.types.Types.FixedType.of(fixedType.length());
      case BINARY:
        return org.apache.gravitino.rel.types.Types.BinaryType.get();
      case DECIMAL:
        Types.DecimalType decimal = (Types.DecimalType) primitive;
        return org.apache.gravitino.rel.types.Types.DecimalType.of(
            decimal.precision(), decimal.scale());
      default:
        return org.apache.gravitino.rel.types.Types.ExternalType.of(primitive.typeId().name());
    }
  }
}
