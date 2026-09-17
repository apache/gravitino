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

package org.apache.gravitino.trino.connector.catalog.jdbc.mysql;

import io.trino.spi.TrinoException;
import io.trino.spi.type.CharType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import java.util.Optional;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Type.Name;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;
import org.apache.gravitino.trino.connector.util.json.JsonCodec;

/** Type transformer between MySQL and Trino */
public class MySQLDataTypeTransformer extends GeneralDataTypeTransformer {
  private static final int MYSQL_CHAR_LENGTH_LIMIT = 255;
  // 65535 / 4 = 16383, in fact, MySQL limit the row size to 65535, and the utf8mb4 character set
  // uses 4 bytes per character. In fact, if a row has several varchar columns, the length of each
  // column should be less than 16383. For more details, please refer to
  // https://dev.mysql.com/doc/refman/8.0/en/char.html
  private static final int MYSQL_VARCHAR_LENGTH_LIMIT = 16383;

  public static final io.trino.spi.type.Type JSON_TYPE =
      JsonCodec.getJsonType(MySQLDataTypeTransformer.class.getClassLoader());

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (type.name() == Name.STRING) {
      return io.trino.spi.type.VarcharType.createUnboundedVarcharType();
    } else if (Name.TIMESTAMP == type.name()) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      // When the precision is unknown (the MySQL catalog reports it only with MySQL Connector/J
      // 8.0.16 or later) fall back to the MySQL default fractional seconds precision of 0.
      int precision =
          timestampType.hasPrecisionSet()
              ? toMySQLFractionalSecondsPrecision(timestampType.precision())
              : TRINO_SECONDS_PRECISION;
      return timestampType.hasTimeZone()
          ? TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision)
          : TimestampType.createTimestampType(precision);
    } else if (Name.TIME == type.name()) {
      Types.TimeType timeType = (Types.TimeType) type;
      // Precision unknown, same fallback as TIMESTAMP above.
      int precision =
          timeType.hasPrecisionSet()
              ? toMySQLFractionalSecondsPrecision(timeType.precision())
              : TRINO_SECONDS_PRECISION;
      return TimeType.createTimeType(precision);
    } else if (Name.EXTERNAL == type.name()) {
      String catalogString = ((Types.ExternalType) type).catalogString();
      return MySQLExternalDataType.safeValueOf(catalogString).getTrinoType();
    }

    return super.getTrinoType(type);
  }

  @Override
  public Optional<io.trino.spi.type.Type> getSupportedType(io.trino.spi.type.Type type) {
    if (type instanceof TimeType
        || type instanceof TimestampType
        || type instanceof TimestampWithTimeZoneType) {
      // The column is created with the precision kept by getGravitinoType, at most 6 for MySQL.
      io.trino.spi.type.Type supported = getTrinoType(getGravitinoType(type));
      return supported.equals(type) ? Optional.empty() : Optional.of(supported);
    }
    return super.getSupportedType(type);
  }

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (typeClass == io.trino.spi.type.CharType.class) {
      CharType charType = (CharType) type;
      if (charType.getLength() > MYSQL_CHAR_LENGTH_LIMIT) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "MySQL does not support the datatype CHAR with the length greater than "
                + MYSQL_CHAR_LENGTH_LIMIT);
      }

      // We do not support the CHAR without a length.
      if (charType.getLength() == 0) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "MySQL does not support the datatype CHAR with the length 0");
      }

      return Types.FixedCharType.of(charType.getLength());
    } else if (typeClass == io.trino.spi.type.VarcharType.class) {
      io.trino.spi.type.VarcharType varcharType = (io.trino.spi.type.VarcharType) type;

      // If the length is not specified, it is a VARCHAR without length, we convert it to a string
      // type.
      if (varcharType.getLength().isEmpty()) {
        return Types.StringType.get();
      }

      int length = varcharType.getLength().get();
      if (length > MYSQL_VARCHAR_LENGTH_LIMIT) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "MySQL does not support the datatype VARCHAR with the length greater than "
                + MYSQL_VARCHAR_LENGTH_LIMIT);
      }
      return Types.VarCharType.of(length);
    } else if (typeClass == JSON_TYPE.getClass()) {
      return Types.ExternalType.of(MySQLExternalDataType.JSON.getMysqlTypeName());
    } else if (io.trino.spi.type.TimeType.class.isAssignableFrom(typeClass)) {
      // Same as the native Trino MySQL connector: time(p) is stored as TIME(p), p capped at 6
      return Types.TimeType.of(toMySQLFractionalSecondsPrecision(((TimeType) type).getPrecision()));
    } else if (io.trino.spi.type.TimestampType.class.isAssignableFrom(typeClass)) {
      // timestamp(p) is stored as DATETIME(p), p capped at 6
      return Types.TimestampType.withoutTimeZone(
          toMySQLFractionalSecondsPrecision(((TimestampType) type).getPrecision()));
    } else if (io.trino.spi.type.TimestampWithTimeZoneType.class.isAssignableFrom(typeClass)) {
      // timestamp(p) with time zone is stored as TIMESTAMP(p), p capped at 6
      return Types.TimestampType.withTimeZone(
          toMySQLFractionalSecondsPrecision(((TimestampWithTimeZoneType) type).getPrecision()));
    }

    return super.getGravitinoType(type);
  }

  /**
   * Maps a time/timestamp precision between Gravitino and Trino. The Trino type must carry the same
   * precision as the MySQL column, otherwise Trino rejects the values read from MySQL (for example,
   * "Expected 0s for digits beyond precision 0").
   *
   * @param precision the precision of the Gravitino or Trino type.
   * @return the precision to use for the other side, capped at the MySQL maximum of 6.
   */
  private static int toMySQLFractionalSecondsPrecision(int precision) {
    // MySQL supports a fractional seconds precision from 0 to 6 (microseconds precision), see
    // https://dev.mysql.com/doc/refman/8.0/en/fractional-seconds.html
    return Math.min(TRINO_MICROS_PRECISION, precision);
  }
}
