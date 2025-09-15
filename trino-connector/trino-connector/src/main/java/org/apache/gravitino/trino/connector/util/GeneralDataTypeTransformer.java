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
package org.apache.gravitino.trino.connector.util;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarcharType;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rel.types.Types.BooleanType;
import org.apache.gravitino.rel.types.Types.DecimalType;
import org.apache.gravitino.rel.types.Types.StructType;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;

/** This class is used to transform datatype between Apache Gravitino and Trino */
public class GeneralDataTypeTransformer {

  // Trino supports time/timestamp precision from 0 to 12 (picoseconds precision).
  // @see
  // https://trino.io/docs/current/language/types.html#date-and-time
  protected static final int TRINO_SECONDS_PRECISION = 0;
  protected static final int TRINO_MILLIS_PRECISION = 3;
  protected static final int TRINO_MICROS_PRECISION = 6;
  protected static final int TRINO_PICOS_PRECISION = 12;

  /**
   * Transforms a Gravitino datatype to a Trino datatype.
   *
   * @param type the Gravitino datatype
   * @return the Trino datatype
   */
  public Type getTrinoType(org.apache.gravitino.rel.types.Type type) {
    switch (type.name()) {
      case BOOLEAN:
        return BOOLEAN;
      case BYTE:
        if (((Types.ByteType) type).signed()) {
          return TinyintType.TINYINT;
        } else {
          return SmallintType.SMALLINT;
        }
      case SHORT:
        if (((Types.ShortType) type).signed()) {
          return SmallintType.SMALLINT;
        } else {
          return INTEGER;
        }
      case INTEGER:
        if (((Types.IntegerType) type).signed()) {
          return INTEGER;
        } else {
          return BIGINT;
        }
      case LONG:
        if (((Types.LongType) type).signed()) {
          return BIGINT;
        } else {
          return io.trino.spi.type.DecimalType.createDecimalType(20, 0);
        }
      case FLOAT:
        return RealType.REAL;
      case DOUBLE:
        return DOUBLE;
      case DECIMAL:
        DecimalType decimalType = (DecimalType) type;
        return io.trino.spi.type.DecimalType.createDecimalType(
            decimalType.precision(), decimalType.scale());
      case FIXEDCHAR:
        return CharType.createCharType(((Types.FixedCharType) type).length());
      case STRING:
        return VARCHAR;
      case VARCHAR:
        return VarcharType.createVarcharType(((Types.VarCharType) type).length());
      case BINARY:
        return VARBINARY;
      case DATE:
        return DATE;
      case TIME:
        Types.TimeType timeType = (Types.TimeType) type;
        if (timeType.hasPrecisionSet()) {
          int precision = timeType.precision();
          if (precision >= TRINO_SECONDS_PRECISION && precision <= TRINO_PICOS_PRECISION) {
            return TimeType.createTimeType(precision);
          } else {
            throw new TrinoException(
                GravitinoErrorCode.GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE,
                "Unsupported time precision for Trino: " + precision);
          }
        }
        // default millis (precision 3) to match Trino default precision
        return TimeType.TIME_MILLIS;
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) type;
        boolean hasTimeZone = timestampType.hasTimeZone();
        if (timestampType.hasPrecisionSet()) {
          int precision = timestampType.precision();
          if (precision >= TRINO_SECONDS_PRECISION && precision <= TRINO_PICOS_PRECISION) {
            return hasTimeZone
                ? TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision)
                : TimestampType.createTimestampType(precision);
          } else {
            throw new TrinoException(
                GravitinoErrorCode.GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE,
                "Unsupported timestamp precision for Trino: " + precision);
          }
        }
        // default millis (precision 3) to match Trino default precision
        return hasTimeZone
            ? TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS
            : TimestampType.TIMESTAMP_MILLIS;
      case LIST:
        return new ArrayType(getTrinoType(((Types.ListType) type).elementType()));
      case MAP:
        Types.MapType mapType = (Types.MapType) type;
        return new MapType(
            getTrinoType(mapType.keyType()),
            getTrinoType(mapType.valueType()),
            new TypeOperators());
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        return gravitinoRowTypeToTrinoRowType(structType);
      case UUID:
        return UuidType.UUID;
      default:
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE,
            "Unsupported gravitino datatype: " + type);
    }
  }

  private RowType gravitinoRowTypeToTrinoRowType(Types.StructType structType) {
    List<Field> fields =
        Arrays.stream(structType.fields())
            .map(field -> new RowType.Field(Optional.of(field.name()), getTrinoType(field.type())))
            .collect(Collectors.toList());
    return RowType.from(fields);
  }

  private StructType trinoRowTypeToGravitinoRowType(RowType rowType) {
    StructType.Field[] fields =
        rowType.getFields().stream()
            .map(
                field ->
                    StructType.Field.nullableField(
                        field.getName().get(), getGravitinoType(field.getType())))
            .toArray(StructType.Field[]::new);

    return StructType.of(fields);
  }

  /**
   * Transforms a Trino datatype to a Gravitino datatype.
   *
   * @param type the Trino datatype
   * @return the Gravitino datatype
   */
  public org.apache.gravitino.rel.types.Type getGravitinoType(Type type) {
    Class<? extends Type> typeClass = type.getClass();

    if (typeClass == io.trino.spi.type.BooleanType.class) {
      return BooleanType.get();
    } else if (typeClass == io.trino.spi.type.TinyintType.class) {
      return Types.ByteType.get();
    } else if (typeClass == io.trino.spi.type.SmallintType.class) {
      return Types.ShortType.get();
    } else if (typeClass == io.trino.spi.type.IntegerType.class) {
      return Types.IntegerType.get();
    } else if (typeClass == io.trino.spi.type.BigintType.class) {
      return Types.LongType.get();
    } else if (typeClass == io.trino.spi.type.RealType.class) {
      return Types.FloatType.get();
    } else if (typeClass == io.trino.spi.type.DoubleType.class) {
      return Types.DoubleType.get();
    } else if (io.trino.spi.type.DecimalType.class.isAssignableFrom(typeClass)) {
      io.trino.spi.type.DecimalType decimalType = (io.trino.spi.type.DecimalType) type;
      return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
    } else if (typeClass == io.trino.spi.type.CharType.class) {
      return Types.FixedCharType.of(((io.trino.spi.type.CharType) type).getLength());
    } else if (typeClass == io.trino.spi.type.VarcharType.class) {
      return Types.VarCharType.of(
          ((io.trino.spi.type.VarcharType) type).getLength().orElse(Integer.MAX_VALUE - 1));
    } else if (typeClass == io.trino.spi.type.VarbinaryType.class) {
      return Types.BinaryType.get();
    } else if (typeClass == io.trino.spi.type.DateType.class) {
      return Types.DateType.get();
    } else if (io.trino.spi.type.TimeType.class.isAssignableFrom(typeClass)) {
      return Types.TimeType.of(((TimeType) type).getPrecision());
    } else if (io.trino.spi.type.TimestampType.class.isAssignableFrom(typeClass)) {
      return Types.TimestampType.withoutTimeZone(((TimestampType) type).getPrecision());
    } else if (io.trino.spi.type.TimestampWithTimeZoneType.class.isAssignableFrom(typeClass)) {
      return Types.TimestampType.withTimeZone(((TimestampWithTimeZoneType) type).getPrecision());
    } else if (typeClass == io.trino.spi.type.ArrayType.class) {
      // Ignore nullability for the type, we could only get nullability from column metadata
      return Types.ListType.of(
          getGravitinoType(((io.trino.spi.type.ArrayType) type).getElementType()), true);
    } else if (typeClass == io.trino.spi.type.MapType.class) {
      io.trino.spi.type.MapType mapType = (io.trino.spi.type.MapType) type;
      // Ignore nullability for the type, we could only get nullability from column metadata
      return Types.MapType.of(
          getGravitinoType(mapType.getKeyType()), getGravitinoType(mapType.getValueType()), true);
    } else if (typeClass == io.trino.spi.type.RowType.class) {
      return trinoRowTypeToGravitinoRowType((io.trino.spi.type.RowType) type);
    } else if (typeClass == io.trino.spi.type.UuidType.class) {
      return Types.UUIDType.get();
    } else {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_UNSUPPORTED_TRINO_DATATYPE,
          "Unsupported Trino datatype: " + type);
    }
  }
}
