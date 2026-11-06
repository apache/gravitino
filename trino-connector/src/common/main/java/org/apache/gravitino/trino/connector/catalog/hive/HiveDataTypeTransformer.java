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

package org.apache.gravitino.trino.connector.catalog.hive;

import io.trino.spi.TrinoException;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.VarcharType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between Apache Hive and Trino */
public class HiveDataTypeTransformer extends GeneralDataTypeTransformer {
  // Max length of Hive varchar is 65535
  private static final int HIVE_VARCHAR_MAX_LENGTH = 65535;
  private static final int HIVE_CHAR_MAX_LENGTH = 255;

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if ((Type.Name.TIMESTAMP == type.name() && ((Types.TimestampType) type).hasTimeZone())
        || Type.Name.TIME == type.name()) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_UNSUPPORTED_GRAVITINO_DATATYPE,
          "Unsupported gravitino datatype: " + type);
    } else if (Type.Name.TIMESTAMP == type.name()) {
      return TimestampType.TIMESTAMP_MILLIS;
    }
    return super.getTrinoType(type);
  }

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (typeClass == VarcharType.class) {
      VarcharType varcharType = (VarcharType) type;
      if (varcharType.getLength().isEmpty()) {
        return Types.StringType.get();
      }

      int length = varcharType.getLength().get();
      if (length > HIVE_VARCHAR_MAX_LENGTH) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Hive does not support the datatype VARCHAR with the length greater than "
                + HIVE_VARCHAR_MAX_LENGTH
                + ", you can use varchar without length instead");
      }

      return Types.VarCharType.of(length);
    } else if (typeClass == io.trino.spi.type.CharType.class) {
      io.trino.spi.type.CharType charType = (io.trino.spi.type.CharType) type;
      if (charType.getLength() > HIVE_CHAR_MAX_LENGTH) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Hive does not support the datatype CHAR with the length greater than "
                + HIVE_CHAR_MAX_LENGTH);
      }

      return Types.FixedCharType.of(charType.getLength());
    } else if (io.trino.spi.type.TimestampType.class.isAssignableFrom(typeClass)) {
      // When creating a table in Hive, the timestamp data type only supports timestamp and
      // timestamp(3)
      // with the precision being 3 (milliseconds precision)
      io.trino.spi.type.TimestampType timestampType = (io.trino.spi.type.TimestampType) type;
      int precision = timestampType.getPrecision();
      if (precision != TRINO_MILLIS_PRECISION) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Incorrect timestamp precision for timestamp("
                + precision
                + "), the configured precision is MILLISECONDS;");
      }
      return Types.TimestampType.withoutTimeZone();
    }

    return super.getGravitinoType(type);
  }
}
