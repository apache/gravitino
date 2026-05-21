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
package org.apache.gravitino.trino.connector.catalog.glue;

import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Type.Name;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.catalog.hive.HiveDataTypeTransformer;

/**
 * Type transformer for the Glue catalog. Normalizes Trino TIME and TIMESTAMP types to microsecond
 * precision (6) before sending to Gravitino, matching Iceberg's type system requirements.
 */
public class GlueDataTypeTransformer extends HiveDataTypeTransformer {

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    Class<? extends io.trino.spi.type.Type> typeClass = type.getClass();
    if (TimeType.class.isAssignableFrom(typeClass)) {
      // Iceberg only supports microsecond (6) precision for time.
      // HiveDataTypeTransformer maps TIME to TIMESTAMP_MILLIS which is wrong for Iceberg tables.
      return Types.TimeType.of(TRINO_MICROS_PRECISION);
    } else if (TimestampWithTimeZoneType.class.isAssignableFrom(typeClass)) {
      // Iceberg supports TIMESTAMP WITH TIME ZONE; Hive does not.
      return Types.TimestampType.withTimeZone(TRINO_MICROS_PRECISION);
    }
    return super.getGravitinoType(type);
  }

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (Name.TIME == type.name()) {
      return TimeType.TIME_MICROS;
    } else if (Name.TIMESTAMP == type.name()) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      if (timestampType.hasTimeZone()) {
        // TIMESTAMP WITH TIME ZONE is Iceberg-only; always microseconds.
        return TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
      } else if (timestampType.hasPrecisionSet()) {
        // Iceberg tables always carry explicit precision; Hive-loaded timestamps do not.
        return TimestampType.TIMESTAMP_MICROS;
      }
      // Hive/Glue TIMESTAMP has no precision set; fall through to TIMESTAMP_MILLIS.
    }
    return super.getTrinoType(type);
  }
}
