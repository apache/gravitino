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
import java.util.OptionalInt;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Type.Name;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.trino.connector.GravitinoErrorCode;
import org.apache.gravitino.trino.connector.util.GeneralDataTypeTransformer;

/** Type transformer between Apache Iceberg and Trino */
public class IcebergDataTypeTransformer extends GeneralDataTypeTransformer {

  private static final int ICEBERG_MICROS_TIMESTAMP_PRECISION = TRINO_MICROS_PRECISION;
  private static final int ICEBERG_NANOS_TIMESTAMP_PRECISION = 9;

  private final int trinoVersion;

  /**
   * Constructs an Iceberg type transformer for a specific Trino runtime.
   *
   * @param trinoVersion the runtime Trino SPI version
   */
  public IcebergDataTypeTransformer(int trinoVersion) {
    this.trinoVersion = trinoVersion;
  }

  @Override
  public Type getGravitinoType(io.trino.spi.type.Type type) {
    if (hasBaseType(type, "unknown")) {
      throw unsupportedIcebergV3Type(IcebergV3Type.UNKNOWN);
    } else if (hasBaseType(type, "variant")) {
      throw unsupportedIcebergV3Type(IcebergV3Type.VARIANT);
    } else if (hasBaseType(type, "geometry")) {
      throw unsupportedIcebergV3Type(IcebergV3Type.GEOMETRY);
    } else if (hasBaseType(type, "geography") || hasBaseType(type, "sphericalgeography")) {
      throw unsupportedIcebergV3Type(IcebergV3Type.GEOGRAPHY);
    }

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
      int precision =
          icebergTimestampPrecision(((TimestampType) type).getPrecision(), type.getDisplayName());
      return Types.TimestampType.withoutTimeZone(precision);
    } else if (io.trino.spi.type.TimestampWithTimeZoneType.class.isAssignableFrom(typeClass)) {
      int precision =
          icebergTimestampPrecision(
              ((TimestampWithTimeZoneType) type).getPrecision(), type.getDisplayName());
      return Types.TimestampType.withTimeZone(precision);
    }

    return super.getGravitinoType(type);
  }

  @Override
  public io.trino.spi.type.Type getTrinoType(Type type) {
    if (Name.NULL == type.name()) {
      throw unsupportedIcebergV3Type(IcebergV3Type.UNKNOWN);
    } else if (Name.FIXED == type.name()) {
      return VarbinaryType.VARBINARY;
    } else if (Name.VARIANT == type.name()) {
      throw unsupportedIcebergV3Type(IcebergV3Type.VARIANT);
    } else if (Name.GEOMETRY == type.name()) {
      throw unsupportedIcebergV3Type(IcebergV3Type.GEOMETRY);
    } else if (Name.GEOGRAPHY == type.name()) {
      throw unsupportedIcebergV3Type(IcebergV3Type.GEOGRAPHY);
    } else if (Name.TIME == type.name()) {
      return TimeType.TIME_MICROS;
    } else if (Name.TIMESTAMP == type.name()) {
      Types.TimestampType timestampType = (Types.TimestampType) type;
      int precision =
          timestampType.hasPrecisionSet()
              ? icebergTimestampPrecision(timestampType.precision(), type.simpleString())
              : ICEBERG_MICROS_TIMESTAMP_PRECISION;
      if (timestampType.hasTimeZone()) {
        return TimestampWithTimeZoneType.createTimestampWithTimeZoneType(precision);
      } else {
        return TimestampType.createTimestampType(precision);
      }
    }

    return super.getTrinoType(type);
  }

  private int icebergTimestampPrecision(int precision, String typeName) {
    if (precision <= ICEBERG_MICROS_TIMESTAMP_PRECISION) {
      return ICEBERG_MICROS_TIMESTAMP_PRECISION;
    }
    if (precision == ICEBERG_NANOS_TIMESTAMP_PRECISION) {
      if (trinoVersion >= IcebergV3Type.NANOSECOND_TIMESTAMP.minimumTrinoVersion.getAsInt()) {
        return ICEBERG_NANOS_TIMESTAMP_PRECISION;
      }
      throw unsupportedIcebergV3Type(IcebergV3Type.NANOSECOND_TIMESTAMP);
    }

    throw new TrinoException(
        GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
        String.format(
            "Iceberg cannot preserve %s; timestamp precision must be %d or %d",
            typeName, ICEBERG_MICROS_TIMESTAMP_PRECISION, ICEBERG_NANOS_TIMESTAMP_PRECISION));
  }

  private static boolean hasBaseType(io.trino.spi.type.Type type, String typeName) {
    return type.getTypeSignature().getBase().equalsIgnoreCase(typeName);
  }

  private TrinoException unsupportedIcebergV3Type(IcebergV3Type type) {
    if (type.minimumTrinoVersion.isEmpty()) {
      return new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          String.format(
              "Iceberg V3 %s is not supported by any Trino Iceberg connector version",
              type.displayName));
    }
    int minimumTrinoVersion = type.minimumTrinoVersion.getAsInt();
    if (trinoVersion < minimumTrinoVersion) {
      return new TrinoException(
          GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
          String.format(
              "Iceberg V3 %s requires Trino %d or newer; current Trino version is %d",
              type.displayName, minimumTrinoVersion, trinoVersion));
    }

    return new TrinoException(
        GravitinoErrorCode.GRAVITINO_ILLEGAL_ARGUMENT,
        String.format(
            "The Gravitino connector cannot losslessly convert Iceberg V3 %s on Trino %d",
            type.displayName, trinoVersion));
  }

  private enum IcebergV3Type {
    NANOSECOND_TIMESTAMP("nanosecond timestamp", 481),
    VARIANT("variant", 481),
    UNKNOWN("unknown"),
    GEOMETRY("geometry", 482),
    GEOGRAPHY("geography", 482);

    private final String displayName;
    private final OptionalInt minimumTrinoVersion;

    IcebergV3Type(String displayName, int minimumTrinoVersion) {
      this.displayName = displayName;
      this.minimumTrinoVersion = OptionalInt.of(minimumTrinoVersion);
    }

    IcebergV3Type(String displayName) {
      this.displayName = displayName;
      this.minimumTrinoVersion = OptionalInt.empty();
    }
  }
}
