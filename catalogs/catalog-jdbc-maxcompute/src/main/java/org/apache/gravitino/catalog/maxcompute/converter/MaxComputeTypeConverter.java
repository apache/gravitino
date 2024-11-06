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
package org.apache.gravitino.catalog.maxcompute.converter;

import org.apache.gravitino.catalog.jdbc.converter.JdbcTypeConverter;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;

public class MaxComputeTypeConverter extends JdbcTypeConverter {

  static final String TINYINT = "tinyint";
  static final String SMALLINT = "smallint";
  static final String INT = "int";
  static final String BIGINT = "bigint";
  static final String BINARY = "binary";
  static final String FLOAT = "float";
  static final String DOUBLE = "double";
  static final String DECIMAL = "decimal";
  static final String VARCHAR = "varchar";
  static final String CHAR = "char";
  static final String STRING = "string";
  static final String DATE = "date";
  static final String DATETIME = "datetime";
  static final String TIMESTAMP = "timestamp";
  static final String BOOLEAN = "boolean";

  @Override
  public Type toGravitino(JdbcTypeBean typeBean) {
    switch (typeBean.getTypeName().toLowerCase()) {
      case TINYINT:
        return Types.ByteType.get();
      case SMALLINT:
        return Types.ShortType.get();
      case INT:
        return Types.IntegerType.get();
      case BIGINT:
        return Types.LongType.get();
      case BINARY:
        return Types.BinaryType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DOUBLE:
        return Types.DoubleType.get();
      case DECIMAL:
        return Types.DecimalType.of(
            Integer.parseInt(typeBean.getColumnSize()), Integer.parseInt(typeBean.getScale()));
      case VARCHAR:
        return Types.VarCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case CHAR:
        return Types.FixedCharType.of(Integer.parseInt(typeBean.getColumnSize()));
      case STRING:
        return Types.StringType.get();
      case DATE:
        return Types.DateType.get();
      case DATETIME:
        return Types.TimestampType.withoutTimeZone();
      case TIMESTAMP:
        return Types.TimestampType.withTimeZone();
      case BOOLEAN:
        return Types.BooleanType.get();
      default:
        return Types.ExternalType.of(typeBean.getTypeName());
    }
  }

  @Override
  public String fromGravitino(Type type) {
    if (type instanceof Types.ByteType) {
      return TINYINT;
    } else if (type instanceof Types.ShortType) {
      return SMALLINT;
    } else if (type instanceof Types.IntegerType) {
      return INT;
    } else if (type instanceof Types.LongType) {
      return BIGINT;
    } else if (type instanceof Types.BinaryType) {
      return type.simpleString();
    } else if (type instanceof Types.FloatType) {
      return type.simpleString();
    } else if (type instanceof Types.DoubleType) {
      return type.simpleString();
    } else if (type instanceof Types.DecimalType) {
      return DECIMAL
          + "("
          + ((Types.DecimalType) type).precision()
          + ","
          + ((Types.DecimalType) type).scale()
          + ")";
    } else if (type instanceof Types.VarCharType) {
      return VARCHAR + "(" + ((Types.VarCharType) type).length() + ")";
    } else if (type instanceof Types.FixedCharType) {
      return CHAR + "(" + ((Types.FixedCharType) type).length() + ")";
    } else if (type instanceof Types.StringType) {
      return STRING;
    } else if (type instanceof Types.DateType) {
      return DATE;
    } else if (type instanceof Types.TimestampType) {
      return ((Types.TimestampType) type).hasTimeZone() ? TIMESTAMP : DATETIME;
    } else if (type instanceof Types.BooleanType) {
      return BOOLEAN;
    } else if (type instanceof Types.ExternalType) {
      return ((Types.ExternalType) type).catalogString();
    }
    throw new IllegalArgumentException(
        String.format(
            "Couldn't convert Gravitino type %s to MaxCompute type", type.simpleString()));
  }
}
