/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gravitino.catalog.lakehouse.paimon.utils;

import java.util.Arrays;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeDefaultVisitor;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;

// Referred to org/apache/paimon/spark/SparkTypeUtils.java
/** Utilities of {@link Type} to support type conversion. */
public class TypeUtils {

  public static final int PRECISION_SECOND = 0;
  public static final int PRECISION_MICROSECOND = 6;

  private TypeUtils() {}

  /**
   * Convert Paimon {@link DataType} data type to Gravitino {@link Type} data type.
   *
   * @param dataType Paimon {@link DataType} data type.
   * @return Gravitino {@link Type} data type.
   */
  public static Type fromPaimonType(DataType dataType) {
    return dataType.accept(PaimonToGravitinoTypeVisitor.INSTANCE);
  }

  /**
   * Convert Gravitino {@link Type} data type to Paimon {@link DataType} data type.
   *
   * @param type Gravitino {@link Type} data type.
   * @return Paimon {@link DataType} data type.
   */
  public static DataType toPaimonType(Type type) {
    return GravitinoToPaimonTypeVisitor.visit(type);
  }

  private static class PaimonToGravitinoTypeVisitor extends DataTypeDefaultVisitor<Type> {

    private static final PaimonToGravitinoTypeVisitor INSTANCE = new PaimonToGravitinoTypeVisitor();

    @Override
    public Type visit(BooleanType booleanType) {
      return Types.BooleanType.get();
    }

    @Override
    public Type visit(TinyIntType tinyIntType) {
      return Types.ByteType.get();
    }

    @Override
    public Type visit(SmallIntType smallIntType) {
      return Types.ShortType.get();
    }

    @Override
    public Type visit(IntType intType) {
      return Types.IntegerType.get();
    }

    @Override
    public Type visit(BigIntType bigIntType) {
      return Types.LongType.get();
    }

    @Override
    public Type visit(FloatType floatType) {
      return Types.FloatType.get();
    }

    @Override
    public Type visit(DoubleType doubleType) {
      return Types.DoubleType.get();
    }

    @Override
    public Type visit(DecimalType decimalType) {
      return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
    }

    @Override
    public Type visit(DateType dateType) {
      return Types.DateType.get();
    }

    @Override
    public Type visit(TimeType timeType) {
      return Types.TimeType.of(timeType.getPrecision());
    }

    @Override
    public Type visit(TimestampType timestampType) {
      return Types.TimestampType.withoutTimeZone(timestampType.getPrecision());
    }

    @Override
    public Type visit(LocalZonedTimestampType localZonedTimestampType) {
      return Types.TimestampType.withTimeZone(localZonedTimestampType.getPrecision());
    }

    @Override
    public Type visit(BinaryType binaryType) {
      return Types.FixedType.of(binaryType.getLength());
    }

    @Override
    public Type visit(VarBinaryType varBinaryType) {
      return Types.BinaryType.get();
    }

    @Override
    public Type visit(VarCharType varCharType) {
      if (varCharType.getLength() == Integer.MAX_VALUE) {
        return Types.StringType.get();
      } else {
        return Types.VarCharType.of(varCharType.getLength());
      }
    }

    @Override
    public Type visit(CharType charType) {
      return Types.FixedCharType.of(charType.getLength());
    }

    @Override
    public Type visit(ArrayType arrayType) {
      return Types.ListType.of(
          arrayType.getElementType().accept(this), arrayType.getElementType().isNullable());
    }

    @Override
    public Type visit(MapType mapType) {
      return Types.MapType.of(
          mapType.getKeyType().accept(this),
          mapType.getValueType().accept(this),
          mapType.getValueType().isNullable());
    }

    @Override
    public Type visit(RowType rowType) {
      return Types.StructType.of(
          rowType.getFields().stream()
              .map(
                  field ->
                      Types.StructType.Field.of(
                          field.name(),
                          field.type().accept(this),
                          field.type().isNullable(),
                          field.description()))
              .toArray(Types.StructType.Field[]::new));
    }

    @Override
    public Type visit(MultisetType multisetType) {
      // Unlike a Java Set, MultisetType allows for multiple instances for each of its
      // elements with a common subtype. And a conversion is possible through a map
      // that assigns each value to an integer to represent the multiplicity of the values.
      // For example, a `MULTISET<INT>` is converted to a `MAP<Integer, Integer>`, the key of the
      // map represents the elements of the Multiset and the value represents the multiplicity of
      // the elements in the Multiset.
      return Types.MapType.of(
          multisetType.getElementType().accept(this), Types.IntegerType.get(), false);
    }

    @Override
    protected Type defaultMethod(DataType dataType) {
      return Types.UnparsedType.of(dataType.asSQLString());
    }
  }

  private static class GravitinoToPaimonTypeVisitor {

    public static DataType visit(Type type) {
      switch (type.name()) {
        case BOOLEAN:
          return DataTypes.BOOLEAN();
        case BYTE:
          return DataTypes.TINYINT();
        case SHORT:
          return DataTypes.SMALLINT();
        case INTEGER:
          return DataTypes.INT();
        case LONG:
          return DataTypes.BIGINT();
        case FLOAT:
          return DataTypes.FLOAT();
        case DOUBLE:
          return DataTypes.DOUBLE();
        case DECIMAL:
          Types.DecimalType decimalType = (Types.DecimalType) type;
          return DataTypes.DECIMAL(decimalType.precision(), decimalType.scale());
        case DATE:
          return DataTypes.DATE();
        case TIME:
          Types.TimeType timeType = (Types.TimeType) type;
          int timeTypePrecision =
              timeType.hasPrecisionSet() ? timeType.precision() : PRECISION_SECOND;
          return DataTypes.TIME(timeTypePrecision);
        case TIMESTAMP:
          Types.TimestampType timestampType = (Types.TimestampType) type;
          int timestampTypePrecision =
              timestampType.hasPrecisionSet() ? timestampType.precision() : PRECISION_MICROSECOND;
          if (timestampType.hasTimeZone()) {
            return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(timestampTypePrecision);
          } else {
            return DataTypes.TIMESTAMP(timestampTypePrecision);
          }
        case STRING:
          return DataTypes.STRING();
        case VARCHAR:
          return DataTypes.VARCHAR(((Types.VarCharType) type).length());
        case FIXEDCHAR:
          return DataTypes.CHAR(((Types.FixedCharType) type).length());
        case FIXED:
          Types.FixedType fixedType = (Types.FixedType) type;
          return DataTypes.BINARY(fixedType.length());
        case BINARY:
          return DataTypes.VARBINARY(VarBinaryType.MAX_LENGTH);
        case LIST:
          Types.ListType listType = (Types.ListType) type;
          DataType elementType = visit(listType.elementType());
          DataType elementTypeWithNullability =
              listType.elementNullable() ? elementType.nullable() : elementType.notNull();
          return DataTypes.ARRAY(elementTypeWithNullability);
        case MAP:
          Types.MapType mapType = (Types.MapType) type;
          DataType keyType = visit(mapType.keyType());
          DataType valueType = visit(mapType.valueType());
          DataType valueTypeWithNullability =
              mapType.valueNullable() ? valueType.nullable() : valueType.notNull();
          return DataTypes.MAP(keyType, valueTypeWithNullability);
        case STRUCT:
          RowType.Builder builder = RowType.builder();
          Arrays.stream(((Types.StructType) type).fields())
              .forEach(
                  field -> {
                    DataType dataType = GravitinoToPaimonTypeVisitor.visit(field.type());
                    DataType dataTypeWithNullable =
                        field.nullable() ? dataType.nullable() : dataType.notNull();
                    builder.field(field.name(), dataTypeWithNullable, field.comment());
                  });
          return builder.build();
        default:
          throw new UnsupportedOperationException(
              String.format(
                  "Paimon does not support Gravitino %s data type.", type.simpleString()));
      }
    }
  }
}
