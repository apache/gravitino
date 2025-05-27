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
package org.apache.gravitino.catalog.postgresql.converter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.rel.types.Types.ListType;

public class PostgreSqlTypeConverter extends JdbcTypeConverter {

  static final String BOOL = "bool";
  static final String INT_2 = "int2";
  static final String INT_4 = "int4";
  static final String INT_8 = "int8";
  static final String FLOAT_4 = "float4";
  static final String FLOAT_8 = "float8";

  static final String TIMESTAMP_TZ = "timestamptz";
  static final String NUMERIC = "numeric";
  static final String BPCHAR = "bpchar";
  static final String BYTEA = "bytea";
  @VisibleForTesting static final String JDBC_ARRAY_PREFIX = "_";
  @VisibleForTesting static final String ARRAY_TOKEN = "[]";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    String typeName = typeBean.getTypeName().toLowerCase();
    if (typeName.startsWith(JDBC_ARRAY_PREFIX)) {
      return toGravitinoArrayType(typeName);
    }
    switch (typeName) {
      case BOOL:
        return Types.BooleanType.get();
      case INT_2:
        return Types.ShortType.get();
      case INT_4:
        return Types.IntegerType.get();
      case INT_8:
        return Types.LongType.get();
      case FLOAT_4:
        return Types.FloatType.get();
      case FLOAT_8:
        return Types.DoubleType.get();
      case DATE:
        return Types.DateType.get();
      case TIME:
        return Optional.ofNullable(typeBean.getDatetimePrecision())
            .map(Types.TimeType::of)
            .orElseGet(Types.TimeType::get);
      case TIMESTAMP:
        return Optional.ofNullable(typeBean.getDatetimePrecision())
            .map(Types.TimestampType::withoutTimeZone)
            .orElseGet(Types.TimestampType::withoutTimeZone);
      case TIMESTAMP_TZ:
        return Optional.ofNullable(typeBean.getDatetimePrecision())
            .map(Types.TimestampType::withTimeZone)
            .orElseGet(Types.TimestampType::withTimeZone);
      case NUMERIC:
        return Types.DecimalType.of(typeBean.getColumnSize(), typeBean.getScale());
      case VARCHAR:
        return Types.VarCharType.of(typeBean.getColumnSize());
      case BPCHAR:
        return Types.FixedCharType.of(typeBean.getColumnSize());
      case TEXT:
        return Types.StringType.get();
      case BYTEA:
        return Types.BinaryType.get();
      default:
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.BooleanType) {
      return BOOL;
    } else if (type instanceof Types.ShortType) {
      return INT_2;
    } else if (type instanceof Types.IntegerType) {
      return INT_4;
    } else if (type instanceof Types.LongType) {
      return INT_8;
    } else if (type instanceof Types.FloatType) {
      return FLOAT_4;
    } else if (type instanceof Types.DoubleType) {
      return FLOAT_8;
    } else if (type instanceof Types.StringType) {
      return TEXT;
    } else if (type instanceof Types.DateType) {
      return type.simpleString();
    } else if (type instanceof Types.TimeType) {
      return type.simpleString();
    } else if (type instanceof Types.TimestampType) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      String baseType = timestampType.hasTimeZone() ? TIMESTAMP_TZ : TIMESTAMP;
      return timestampType.hasPrecisionSet()
          ? String.format("%s(%d)", baseType, timestampType.precision())
          : baseType;
    } else if (type instanceof Types.DecimalType) {
      return NUMERIC
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.VarCharType) {
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.FixedCharType) {
      return BPCHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.BinaryType) {
      return BYTEA;
    } else if (type instanceof Types.ListType) {
      return fromGravitinoArrayType((ListType) type);
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format(
            "Couldn't convert Gravitino type %s to PostgreSQL type", type.simpleString()));
  }

  // PG doesn't support the multidimensional array internally. The current implementation does not
  // enforce the declared number of dimensions either. Arrays of a particular element type are all
  // considered to be of the same type, regardless of size or number of dimensions. So, declaring
  // the array size or number of dimensions in CREATE TABLE is simply documentation; it does not
  // affect run-time behavior.
  // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-DECLARATION
  private String fromGravitinoArrayType(ListType listType) {
    Type elementType = listType.elementType();
    Preconditions.checkArgument(
        !listType.elementNullable(), "PostgreSQL doesn't support element to nullable");
    Preconditions.checkArgument(
        !(elementType instanceof ListType),
        "PostgreSQL doesn't support multidimensional list internally, please use one dimensional list");
    String elementTypeString = fromGravitino(elementType);
    return elementTypeString + ARRAY_TOKEN;
  }

  private ListType toGravitinoArrayType(String typeName) {
    String elementTypeName = typeName.substring(JDBC_ARRAY_PREFIX.length(), typeName.length());
    JdbcTypeBean bean = new JdbcTypeBean(elementTypeName);
    return ListType.of(toGravitino(bean), false);
  }
}
