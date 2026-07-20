/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.starrocks.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

/** Type converter for StarRocks. */
public class StarRocksTypeConverter extends JdbcTypeConverter {

  static final String BIGINT = "bigint";
  static final String BOOLEAN = "boolean";
  static final String DECIMAL = "decimal";
  static final String DOUBLE = "double";
  static final String FLOAT = "float";
  static final String INT = "int";
  static final String LARGEINT = "largeint";
  static final String SMALLINT = "smallint";
  static final String TINYINT = "tinyint";

  static final String BINARY = "binary";
  static final String VARBINARY = "varbinary";
  static final String CHAR = "char";
  static final String STRING = "string";
  static final String VARCHAR = "varchar";

  static final String DATE = "date";
  static final String DATETIME = "datetime";

  static final String ARRAY = "array";
  static final String JSON = "json";
  static final String MAP = "map";
  static final String STRUCT = "struct";

  static final String BITMAP = "bitmap";
  static final String HLL = "hll";

  static final String BIT = "BIT";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    switch (typeBean.getTypeName().toLowerCase()) {
      case BIGINT:
        return Types.LongType.get();
      case BOOLEAN:
        return Types.BooleanType.get();
      case DECIMAL:
        return Types.DecimalType.of(typeBean.getColumnSize(), typeBean.getScale());
      case DOUBLE:
        return Types.DoubleType.get();
      case FLOAT:
        return Types.FloatType.get();
      case INT:
        return Types.IntegerType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case TINYINT:
        return Types.ByteType.get();
      case BINARY:
      case VARBINARY:
        return Types.BinaryType.get();
      case CHAR:
        return Types.FixedCharType.of(typeBean.getColumnSize());
      case STRING:
        return Types.StringType.get();
      case VARCHAR:
        if (typeBean.getColumnSize() == 65533) {
          return Types.StringType.get();
        }
        return Types.VarCharType.of(typeBean.getColumnSize());
      case DATE:
        return Types.DateType.get();
      case DATETIME:
        return Types.TimestampType.withoutTimeZone();
      default:
        if (typeBean.getTypeName().equals("BIT")
            && typeBean.getColumnSize() == 1
            && typeBean.getScale() == 0) {
          return Types.BooleanType.get();
        }
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.LongType) {
      return BIGINT;
    } else if (type instanceof Types.BooleanType) {
      return BOOLEAN;
    } else if (type instanceof Types.DecimalType) {
      return DECIMAL
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.DoubleType) {
      return DOUBLE;
    } else if (type instanceof Types.FloatType) {
      return FLOAT;
    } else if (type instanceof Types.IntegerType) {
      return INT;
    } else if (type instanceof Types.ShortType) {
      return SMALLINT;
    } else if (type instanceof Types.ByteType) {
      return TINYINT;
    } else if (type instanceof Types.BinaryType) {
      return BINARY;
    } else if (type instanceof Types.FixedCharType) {
      int length = ((Types.FixedCharType) type).length();
      if (length < 1 || length > 255) {
        throw new IllegalArgumentException(
            String.format(
                "Type %s is invalid, length should be between 1 and 255", type.simpleString()));
      }

      return CHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.StringType) {
      return STRING;
    } else if (type instanceof Types.VarCharType) {
      int length = ((Types.VarCharType) type).length();
      if (length < 1 || length > 1048576) {
        throw new IllegalArgumentException(
            String.format(
                "Type %s is invalid, length should be between 1 and 1048576", type.simpleString()));
      }
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.DateType) {
      return DATE;
    } else if (type instanceof Types.TimestampType) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      if (timestampType.hasTimeZone()) {
        throw new IllegalArgumentException(
            String.format(
                "StarRocks DATETIME does not preserve time-zone semantics; cannot convert Gravitino type %s",
                type.simpleString()));
      }
      if (timestampType.hasPrecisionSet()) {
        throw new IllegalArgumentException(
            String.format(
                "StarRocks DATETIME columns do not preserve declared fractional precision; cannot convert Gravitino type %s",
                type.simpleString()));
      }
      return DATETIME;
    } else if (type instanceof Types.VariantType) {
      throw new IllegalArgumentException(
          "StarRocks JSON is not an exact representation of Gravitino Variant; cannot convert Gravitino type variant");
    } else if (type instanceof Types.NullType) {
      throw new IllegalArgumentException(
          "StarRocks table columns cannot represent Gravitino Unknown (NullType); cannot convert Gravitino type null");
    } else if (type instanceof Types.GeometryType) {
      throw new IllegalArgumentException(
          String.format(
              "StarRocks has no storable GEOMETRY column type with CRS metadata; cannot convert Gravitino type %s",
              type.simpleString()));
    } else if (type instanceof Types.GeographyType) {
      throw new IllegalArgumentException(
          String.format(
              "StarRocks has no storable GEOGRAPHY column type with CRS and edge-algorithm metadata; cannot convert Gravitino type %s",
              type.simpleString()));
    }
    throw new IllegalArgumentException(
        String.format("Couldn't convert Gravitino type %s to StarRocks type", type.simpleString()));
  }
}
