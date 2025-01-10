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

package org.apache.gravitino.trino.connector.catalog.jdbc.postgresql;

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

/** Type transformer between PostgreSQL and Trino */
public class PostgreSQLDataTypeTransformer extends GeneralDataTypeTransformer {
  @SuppressWarnings("UnusedVariable")
  private static final int POSTGRESQL_CHAR_LENGTH_LIMIT = 10485760;
  // 1 GB, please refer to
  // https://stackoverflow.com/questions/70785582/is-a-varchar-unlimited-in-postgresql
  @SuppressWarnings("UnusedVariable")
  private static final int POSTGRESQL_VARCHAR_LENGTH_LIMIT = 10485760;

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (type.name() == Name.STRING) {
      return io.trino.spi.type.VarcharType.createUnboundedVarcharType();
    } else if (Name.TIMESTAMP == type.name()) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      if (timestampType.hasTimeZone()) {
        return TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
      } else {
        return TimestampType.TIMESTAMP_MICROS;
      }
    } else if (Name.TIME == type.name()) {
      return TimeType.TIME_MICROS;
    }

    return super.getTrinoType(type);
  }

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (typeClass == io.trino.spi.type.CharType.class) {
      CharType charType = (CharType) type;

      // Do not need to check the scenario that the length of the CHAR type is greater than
      // POSTGRESQL_CHAR_LENGTH_LIMIT ,because the length of the CHAR type in Trino is no greater
      // than 65536 We do not support the CHAR without a length.
      if (charType.getLength() == 0) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "PostgreSQL does not support the datatype CHAR with the length 0");
      }

      return Types.FixedCharType.of(charType.getLength());
    } else if (typeClass == io.trino.spi.type.VarcharType.class) {
      io.trino.spi.type.VarcharType varcharType = (io.trino.spi.type.VarcharType) type;

      // If the length is not specified, it is a VARCHAR without length, we convert it to a string
      // type.
      if (varcharType.getLength().isEmpty()) {
        return Types.StringType.get();
      }

      return Types.VarCharType.of(varcharType.getLength().get());
    }

    return super.getGravitinoType(type);
  }
}
