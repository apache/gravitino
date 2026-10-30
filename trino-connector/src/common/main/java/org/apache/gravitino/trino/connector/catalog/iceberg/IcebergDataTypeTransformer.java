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

package org.apache.gravitino.trino.connector.catalog.iceberg;

import io.trino.spi.TrinoException;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Type.Name;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between Apache Iceberg and Trino */
public class IcebergDataTypeTransformer extends GeneralDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (typeClass == io.trino.spi.type.CharType.class) {
      throw new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          "Iceberg does not support the datatype CHAR");
    } else if (typeClass == io.trino.spi.type.VarcharType.class) {
      VarcharType varCharType = (VarcharType) type;
      if (varCharType.getLength().isPresent()) {
        throw new TrinoException(
            GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
            "Iceberg does not support the datatype VARCHAR with length");
      }
      return Types.StringType.get();
    } else if (io.trino.spi.type.TimeType.class.isAssignableFrom(typeClass)) {
      // Iceberg only supports time type with microsecond (6) precision
      return Types.TimeType.of(TRINO_MICROS_PRECISION);
    } else if (io.trino.spi.type.TimestampType.class.isAssignableFrom(typeClass)) {
      // Iceberg only supports timestamp type (without time zone) with microsecond (6) precision
      return Types.TimestampType.withoutTimeZone(TRINO_MICROS_PRECISION);
    } else if (io.trino.spi.type.TimestampWithTimeZoneType.class.isAssignableFrom(typeClass)) {
      // Iceberg only supports timestamp with time zone type with microsecond (6) precision
      return Types.TimestampType.withTimeZone(TRINO_MICROS_PRECISION);
    }

    return super.getGravitinoType(type);
  }

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (Name.FIXED == type.name()) {
      return VarbinaryType.VARBINARY;
    } else if (Name.TIME == type.name()) {
      return TimeType.TIME_MICROS;
    } else if (Name.TIMESTAMP == type.name()) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      if (timestampType.hasTimeZone()) {
        return TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
      } else {
        return TimestampType.TIMESTAMP_MICROS;
      }
    }

    return super.getTrinoType(type);
  }
}
