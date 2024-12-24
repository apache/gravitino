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
package org.apache.gravitino.catalog.clickhouse.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Type converter for ClickHouse. */
public class ClickHouseTypeConverter extends JdbcTypeConverter {

  static final String INT8 = "Int8";
  static final String INT16 = "Int16";
  static final String INT32 = "Int32";
  static final String INT64 = "Int64";
  static final String INT128 = "Int128";
  static final String INT256 = "Int256";
  static final String UINT8 = "UInt8";
  static final String UINT16 = "UInt16";
  static final String UINT32 = "UInt32";
  static final String UINT64 = "UInt64";
  static final String UINT128 = "UInt128";
  static final String UINT256 = "UInt256";

  static final String FLOAT32 = "Float32";
  static final String FLOAT64 = "Float64";
  static final String BFLOAT16 = "BFloat16";
  static final String DECIMAL = "Decimal";
  static final String STRING = "String";
  static final String FIXEDSTRING = "FixedString";
  static final String DATE = "Date";
  static final String DATE32 = "Date32";
  static final String DATETIME = "DateTime";
  static final String DATETIME64 = "DateTime64";
  static final String ENUM = "Enum";
  static final String BOOL = "Bool";
  static final String UUID = "UUID";

  // bellow is Object Data Type
  static final String IPV4 = "IPv4";
  static final String IPV6 = "IPv6";
  static final String ARRAY = "Array";
  static final String TUPLE = "Tuple";
  static final String MAP = "Map";
  static final String VARIANT = "Variant";
  static final String LOWCARDINALITY = "LowCardinality";
  static final String NULLABLE = "Nullable";
  static final String AGGREGATEFUNCTION = "AggregateFunction";
  static final String SIMPLEAGGREGATEFUNCTION = "SimpleAggregateFunction";
  static final String GEO = "Geo";

  // bellow is Special Data Types
  static final String Domains = "Domains";
  static final String Nested = "Nested";
  static final String Dynamic = "Dynamic";
  static final String JSON = "JSON";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    String typeName = typeBean.getTypeName();
    if (typeName.startsWith("Nullable(")) {
      typeName = typeName.substring(9, typeName.length() - 1);
    }

    if (typeName.startsWith("Decimal(")) {
      typeName = "Decimal";
    }

    if (typeName.startsWith("FixedString(")) {
      typeName = "FixedString";
    }

    switch (typeName) {
      case INT8:
        return Types.ByteType.get();
      case INT16:
        return Types.ShortType.get();
      case INT32:
        return Types.IntegerType.get();
      case INT64:
        return Types.LongType.get();
      case UINT8:
        return Types.ByteType.unsigned();
      case UINT16:
        return Types.ShortType.unsigned();
      case UINT32:
        return Types.IntegerType.unsigned();
      case UINT64:
        return Types.LongType.unsigned();
      case FLOAT32:
        return Types.FloatType.get();
      case FLOAT64:
        return Types.DoubleType.get();
      case DECIMAL:
        return Types.DecimalType.of(typeBean.getColumnSize(), typeBean.getScale());
      case STRING:
        return Types.StringType.get();
      case FIXEDSTRING:
        return Types.FixedCharType.of(typeBean.getColumnSize());
      case DATE:
        return Types.DateType.get();
      case DATE32:
        return Types.DateType.get();
      case DATETIME:
        return Types.TimestampType.withoutTimeZone();
      case DATETIME64:
        return Types.TimestampType.withoutTimeZone();
      case BOOL:
        return Types.BooleanType.get();
      case UUID:
        return Types.UUIDType.get();
      default:
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.ByteType) {
      if (((Types.ByteType) type).signed()) {
        return INT8;
      } else {
        return UINT8;
      }
    } else if (type instanceof Types.ShortType) {
      if (((Types.ShortType) type).signed()) {
        return INT16;
      } else {
        return UINT16;
      }
    } else if (type instanceof Types.IntegerType) {
      if (((Types.IntegerType) type).signed()) {
        return INT32;
      } else {
        return UINT32;
      }
    } else if (type instanceof Types.LongType) {
      if (((Types.LongType) type).signed()) {
        return INT64;
      } else {
        return UINT64;
      }
    } else if (type instanceof Types.FloatType) {
      return FLOAT32;
    } else if (type instanceof Types.DoubleType) {
      return FLOAT64;
    } else if (type instanceof Types.StringType) {
      return STRING;
    } else if (type instanceof Types.DateType) {
      return DATE;
    } else if (type instanceof Types.TimestampType) {
      return DATETIME;
    } else if (type instanceof Types.TimeType) {
      return INT64;
    } else if (type instanceof Types.DecimalType) {
      return DECIMAL
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.VarCharType) {
      return STRING;
    } else if (type instanceof Types.FixedCharType) {
      return FIXEDSTRING + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.BooleanType) {
      return BOOL;
    } else if (type instanceof Types.UUIDType) {
      return UUID;
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format(
            "Couldn't convert Gravitino type %s to ClickHouse type", type.simpleString()));
  }
}
