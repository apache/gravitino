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
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Type.Name;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between MySQL and Trino */
public class MySQLDataTypeTransformer extends GeneralDataTypeTransformer {
  private static final int MYSQL_CHAR_LENGTH_LIMIT = 255;
  // 65535 / 4 = 16383, in fact, MySQL limit the row size to 65535, and the utf8mb4 character set
  // uses 4 bytes per character. In fact, if a row has several varchar columns, the length of each
  // column should be less than 16383. For more details, please refer to
  // https://dev.mysql.com/doc/refman/8.0/en/char.html
  private static final int MYSQL_VARCHAR_LENGTH_LIMIT = 16383;
  private static final int TIMESTAMP_PRECISION_SECONDS = 0;
  private static final int TIMESTAMP_PRECISION_MILLIS = 3;
  private static final int TIMESTAMP_PRECISION_MICROS = 6;

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (type.name() == Name.STRING) {
      return io.trino.spi.type.VarcharType.createUnboundedVarcharType();
    } else if (Name.TIMESTAMP == type.name()) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      return timestampType.hasTimeZone()
          ? getTimestampWithTimeZoneType(timestampType)
          : getTimestampType(timestampType);
    } else if (Name.TIME == type.name()) {
      return getTimeType(((Types.TimeType) type));
    }
    return super.getTrinoType(type);
  }

  private static TimestampWithTimeZoneType getTimestampWithTimeZoneType(
      Types.TimestampType timestampType) {
    if (!timestampType.hasPrecision()) {
      return TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
    }
    switch (timestampType.precision()) {
      case TIMESTAMP_PRECISION_SECONDS:
        return TimestampWithTimeZoneType.TIMESTAMP_TZ_SECONDS;
      case TIMESTAMP_PRECISION_MILLIS:
        return TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
      case TIMESTAMP_PRECISION_MICROS:
        return TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
      default:
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Invalid MySQL timestamp precision: "
                + timestampType.precision()
                + ". Valid values are 0, 3, 6");
    }
  }

  private static TimestampType getTimestampType(Types.TimestampType timestampType) {
    if (!timestampType.hasPrecision()) {
      return TimestampType.TIMESTAMP_SECONDS;
    }
    switch (timestampType.precision()) {
      case TIMESTAMP_PRECISION_SECONDS:
        return TimestampType.TIMESTAMP_SECONDS;
      case TIMESTAMP_PRECISION_MILLIS:
        return TimestampType.TIMESTAMP_MILLIS;
      case TIMESTAMP_PRECISION_MICROS:
        return TimestampType.TIMESTAMP_MICROS;
      default:
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Invalid MySQL datetime precision: "
                + timestampType.precision()
                + ". Valid values are 0, 3, 6");
    }
  }

  private static TimeType getTimeType(Types.TimeType timeType) {
    if (!timeType.hasPrecision()) {
      return TimeType.TIME_SECONDS;
    }
    switch (timeType.precision()) {
      case TIMESTAMP_PRECISION_SECONDS:
        return TimeType.TIME_SECONDS;
      case TIMESTAMP_PRECISION_MILLIS:
        return TimeType.TIME_MILLIS;
      case TIMESTAMP_PRECISION_MICROS:
        return TimeType.TIME_MICROS;
      default:
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Invalid MySQL time precision: " + timeType.precision() + ". Valid values are 0, 3, 6");
    }
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
    }

    return super.getGravitinoType(type);
  }
}
